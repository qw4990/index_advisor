package main

import (
	"fmt"
	"testing"
)

func TestFindIndexableColumnsSimple(t *testing.T) {
	workload := WorkloadInfo{
		TableSchemas: ListToSet(TableSchema{"test", "t", NewColumns("test", "t", "a", "b", "c", "d", "e"), nil, ""}),
		SQLs: ListToSet(SQL{"test", "select * from t where a<1 and b>1 and e like 'abc'", 1, nil, nil},
			SQL{"test", "select * from t where c in (1, 2, 3) order by d", 1, nil, nil}),
	}
	must(IndexableColumnsSelectionSimple(&workload))
	fmt.Println(workload.IndexableColumns.ToList())
	for _, sql := range workload.SQLs.ToList() {
		fmt.Println(sql.IndexableColumns.ToList())
	}
}
