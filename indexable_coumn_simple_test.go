package main

import (
	"fmt"
	"testing"
)

func TestFindIndexableColumnsSimple(t *testing.T) {
	cols, err := FindIndexableColumnsSimple(WorkloadInfo{
		SQLs: []SQL{
			{
				"select * from t where a<1 and b>1",
				1,
			},
			{
				"select * from t where c in (1, 2, 3) order by d",
				1,
			},
		},
	})
	if err != nil {
		t.Fatal(err)
	}

	fmt.Println(">>> ", cols)
}
