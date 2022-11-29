package main

import (
	"hash/fnv"
	"sort"
	"strconv"
	"strings"
)

// on VScode: go build -buildmode=plugin -gcflags="all=-N -l" -o ./playerPluginrun/playerPlugin.so ./playerPlugin/playerPlugin.go
// on orions: go build -buildmode=plugin -o ./playerPluginrun/playerPlugin.so ./playerPlugin/playerPlugin.go

// map: read data and add each playerName-score pair into a map, then return a list of maps. Each map only contains one playerName-score pair.
func Map(line_number int, line_text string) []*map[string]uint32 {
	var mapped []*map[string]uint32
	words := strings.Split(line_text, ",")
	if len(words) == 2 {
		singleMap := make(map[string]uint32)
		score, err := strconv.ParseFloat(words[1], 32)
		if err != nil {
			return mapped
		}
		singleMap[words[0]] = uint32(score)
		mapped = append(mapped, &singleMap)
	}
	return mapped
}

// shuffle: takes map's result as input, shuffle data by players' name's hash value. Then distribute data to different reducers.
//
//	Output is a map, which key is the reducers' index and value is a list of maps, like map phrase, each map contains one playerName-score pair.
func Shuffle(mappedList []*map[string]uint32, reducernum uint32) map[int]([]*map[string]uint32) {
	shuffled := make(map[int]([]*map[string]uint32))
	for i := 0; i < int(reducernum); i++ {
		emptyList := []*map[string]uint32{}
		shuffled[i] = emptyList
	}
	for _, mapped := range mappedList {
		for k := range *mapped {
			h := fnv.New32a()
			h.Write([]byte(k))
			hashNum := h.Sum32()
			reducer := hashNum % reducernum
			shuffled[int(reducer)] = append(shuffled[int(reducer)], mapped)
		}
	}
	return shuffled
}

// sort: takes shuffle's output as input, output is a list of maps, the key of map is player's name and value is a list of the corresponding player's scores.
func Sort(shuffledList []*map[string]uint32) []*map[string][]uint32 {
	grouped := make(map[string][]uint32)
	for _, shuffled := range shuffledList {
		for k, v := range *shuffled {
			if _, ok := grouped[k]; ok {
				grouped[k] = append(grouped[k], v)
			} else {
				listOfValue := []uint32{v}
				grouped[k] = listOfValue
			}
		}
	}
	sorted := []*map[string][]uint32{}
	sortedKeys := []string{}
	for key := range grouped {
		sortedKeys = append(sortedKeys, key)
	}
	sort.Strings(sortedKeys)
	for _, key := range sortedKeys {
		temp := make(map[string][]uint32)
		temp[key] = grouped[key]
		sorted = append(sorted, &temp)
	}
	return sorted
}

// reduce: takes the output of the sort phrase as input, and then calculates the total-score and average-score per game for each player.
//
//	So the output is a list of maps, the key is players' name and the value is the PTS（Points Per Game） of the corresponding player.
func Reduce(sorted []*map[string][]uint32) []*map[string]uint32 {
	reduced := []*map[string]uint32{}
	for _, m := range sorted {
		for key, value := range *m {
			totalscore := uint32(0)
			games := len(value)
			for _, score := range value {
				totalscore += score
			}
			averageScore := int(totalscore) / games
			temp := make(map[string]uint32)
			temp[key] = uint32(averageScore)
			reduced = append(reduced, &temp)
		}
	}
	return reduced
}
