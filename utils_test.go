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
