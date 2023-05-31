package main

import (
	"fmt"
	"testing"
)

func TestLoadWorkloadInfo(t *testing.T) {
	w, err := LoadWorkloadInfo("test", "./workload/test")
	must(err)
	fmt.Println(w)
}

func TestLoadWorkloadInfoTPCH(t *testing.T) {
	w, err := LoadWorkloadInfo("tpch", "./workload/tpch_1g")
	must(err)
	fmt.Println(len(w.SQLs))
}

func TestCombSet(t *testing.T) {
	s := NewSet[Column]()
	for i := 0; i < 6; i++ {
		s.Add(NewColumn("test", "test", fmt.Sprintf("col%d", i)))
	}

	for i := 1; i < 6; i++ {
		fmt.Println("======================== ", i, " ========================")
		result := CombSet(s, i)
		fmt.Println("--> ", len(result))
		for _, ss := range result {
			fmt.Println(ss.ToList())
		}
	}
}
