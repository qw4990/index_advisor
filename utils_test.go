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
