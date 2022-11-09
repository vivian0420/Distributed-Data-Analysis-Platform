package main

import (
	"hash/fnv"
	"sort"
	"strconv"
	"strings"
)

// go build -buildmode=plugin -gcflags="all=-N -l" -o ./playerPluginrun/playerPlugin.so ./playerPlugin/playerPlugin.go
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

func Sort(shuffledList []*map[string]uint32) []*map[string][]uint32 {
	grouped := make(map[string][]uint32)
	for _, shuffled := range shuffledList {
		for k := range *shuffled {
			if _, ok := grouped[k]; ok {
				grouped[k] = append(grouped[k], 1)
			} else {
				listOfValue := []uint32{1}
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

func Reduce(sorted []*map[string][]uint32) []*map[string]uint32 {
	reduced := []*map[string]uint32{}
	for _, m := range sorted {
		for key, value := range *m {
			totalscore := uint32(0)
			for _, score := range value {
				totalscore += score
			}
			temp := make(map[string]uint32)
			temp[key] = totalscore
			reduced = append(reduced, &temp)
		}
	}
	return reduced
}
