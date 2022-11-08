package main

import (
	"hash/fnv"
	"strings"
)

func Map(line_number int, line_text string) []*map[string]uint32 {
	var mapped []*map[string]uint32
	words := strings.Fields(line_text)
	for _, word := range words {
		singleMap := make(map[string]uint32)
		singleMap[word] = 1
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

func Sort(shuffledList []*map[string]uint32) map[string][]uint32 {
	sorted := make(map[string][]uint32)
	for _, shuffled := range shuffledList {
		for k := range *shuffled {
			if _, ok := sorted[k]; ok {
				sorted[k] = append(sorted[k], 1)
			} else {
				listOfValue := []uint32{1}
				sorted[k] = listOfValue
			}
		}
	}
	return sorted
}

func Reduce(sorted map[string][]uint32) map[string]uint32 {
	reduced := make(map[string]uint32)
	for key, value := range sorted {
		reduced[key] = uint32(len(value))
	}
	return reduced
}
