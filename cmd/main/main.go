package main

import (
	"fmt"
	"sort"
	te "tiny-elephant/tiny-elephant"
)

func main() {
	data := map[string][]string{
		"user1":  {"airplane", "banana", "cat", "dog", "elephant", "fruit", "google", "hobby", "internet", "jogging"},
		"user2":  {"cat", "dog", "elephant", "fruit", "google", "jogging", "kotlin"},
		"user3":  {"java", "rx", "yahoo", "zoo"},
		"user4":  {"apple", "banana"},
		"user5":  {"airplane"},
		"user6":  {"bobby", "dog"},
		"user7":  {"train", "cat", "exercise", "healthy"},
		"user8":  {"healthy", "dog", "exercise", "banana", "youtube"},
		"user9":  {"java", "javascript", "rx", "zoo", "yahoo", "google", "github"},
		"user10": {"cook", "bobby", "dog", "youtube"},
		"user11": {"dance", "airplane", "trip", "elephant", "fruit", "google"},
	}

	imc := te.NewInMemoryCluster(
		"localhost:6379",
		1,
		128,
		1,
		10000,
	)

	imc.FlushDb()
	imc.UpdateCluster(data)

	var keys []string
	for key := range data {
		keys = append(keys, key)
	}

	sort.Strings(keys)

	for _, key := range keys {
		fmt.Println(key)
		fmt.Println(imc.MostCommon(key, 5))
	}

	newData := map[string][]string{
		"user1": {"airplane", "banana", "cat"},
		"user5": {"hobby", "internet", "jogging", "banana", "cat", "dog"},
	}

	imc.UpdateCluster(newData)

	fmt.Println("======== UPDATED!! =========")

	for _, key := range keys {
		fmt.Println(key)
		fmt.Println(imc.MostCommon(key, 5))
	}
}
