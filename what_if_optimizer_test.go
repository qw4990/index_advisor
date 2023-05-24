package main

import (
	"fmt"
	"testing"
)

func TestWhatIfOptimizer(t *testing.T) {
	dsn := "root:@tcp(127.0.0.1:4000)/test"
	o, err := NewTiDBWhatIfOptimizer(dsn)
	must(err)
	defer o.Close()
	must(o.Execute(`create table t (a int, b int)`))
	cost1, err := o.GetPlanCost(`select * from t where a=1`)
	must(err)
	must(o.CreateHypoIndex("test", "t", "idx_a", []string{"a"}))
	cost2, err := o.GetPlanCost(`select * from t where a=1`)
	must(err)
	must(o.DropHypoIndex("test", "t", "idx_a"))
	cost3, err := o.GetPlanCost(`select * from t where a=1`)
	must(err)
	fmt.Println(cost1, cost2, cost3) // cost2 > cost1 = cost3
}

func must(err error) {
	if err != nil {
		panic(err)
	}
}
