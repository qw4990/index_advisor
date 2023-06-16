package optimizer

import (
	"fmt"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"github.com/qw4990/index_advisor/workload"
)

// WhatIfOptimizerStats records the statistics of a what-if optimizer.
type WhatIfOptimizerStats struct {
	ExecuteCount             int           // number of executed SQL statements
	ExecuteTime              time.Duration // total execution time
	CreateOrDropHypoIdxCount int           // number of executed CreateHypoIndex/DropHypoIndex
	CreateOrDropHypoIdxTime  time.Duration // total execution time of CreateHypoIndex/DropHypoIndex
	GetCostCount             int           // number of executed GetCost
	GetCostTime              time.Duration // total execution time of GetCost
}

// Format formats the statistics.
func (s WhatIfOptimizerStats) Format() string {
	return fmt.Sprintf(`Execute(count/time): (%v/%v), CreateOrDropHypoIndex: (%v/%v), GetCost: (%v/%v)`,
		s.ExecuteCount, s.ExecuteTime, s.CreateOrDropHypoIdxCount, s.CreateOrDropHypoIdxTime, s.GetCostCount, s.GetCostTime)
}

// WhatIfOptimizer is the interface of a what-if optimizer.
type WhatIfOptimizer interface {
	Execute(sql string) error // execute the specified SQL statement
	Close() error             // release the underlying database connection

	CreateHypoIndex(index workload.Index) error // create a hypothetical index
	DropHypoIndex(index workload.Index) error   // drop a hypothetical index

	Explain(query string) (plan workload.Plan, err error)        // return the execution plan of the specified query
	ExplainAnalyze(query string) (plan workload.Plan, err error) // return the execution plan of the specified query with analyze

	ResetStats()                 // reset the statistics
	Stats() WhatIfOptimizerStats // return the statistics

	SetDebug(flag bool) // print each query if set to true
}
