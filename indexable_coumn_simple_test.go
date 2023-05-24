package main

import (
	"reflect"
	"sort"
	"testing"
)

func TestFindIndexableColumnsSimple(t *testing.T) {
	cols, err := FindIndexableColumnsSimple(WorkloadInfo{
		TableSchemas: []TableSchema{
			{"test", "t", []string{"a", "b", "c", "d", "e"}, ""},
		},
		SQLs: []SQL{
			{"test", "select * from t where a<1 and b>1 and e like 'abc'", 1},
			{"test", "select * from t where c in (1, 2, 3) order by d", 1},
		},
	})
	if err != nil {
		t.Fatal(err)
	}

	var keys []string
	for _, c := range cols {
		keys = append(keys, c.String())
	}
	sort.Strings(keys)
	if !reflect.DeepEqual(keys, []string{"test.t.a", "test.t.b", "test.t.c", "test.t.d"}) {
		t.Fatal("unexpected")
	}
}
