package optimizer

import (
	"fmt"
	"testing"
	
	"github.com/qw4990/index_advisor/utils"
	"github.com/qw4990/index_advisor/workload"
)

func TestWhatIfOptimizer(t *testing.T) {
	dsn := "root:@tcp(127.0.0.1:4000)/test"
	o, err := NewTiDBWhatIfOptimizer(dsn)
	utils.Must(err)
	defer o.Close()
	utils.Must(o.Execute(`create table t (a int, b int)`))
	p1, err := o.Explain(`select * from t where a=1`)
	utils.Must(err)
	utils.Must(o.CreateHypoIndex(workload.NewIndex("test", "t", "idx_a", "a")))
	p2, err := o.Explain(`select * from t where a=1`)
	utils.Must(err)
	utils.Must(o.DropHypoIndex(workload.NewIndex("test", "t", "idx_a", "a")))
	p3, err := o.Explain(`select * from t where a=1`)
	utils.Must(err)
	fmt.Println(p1.PlanCost(), p2.PlanCost(), p3.PlanCost()) // cost2 > cost1 = cost3
}
