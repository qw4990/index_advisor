package main

import (
	"fmt"
	"sort"
)

// IndexSelectionAlgo is the interface for index selection algorithms.
type IndexSelectionAlgo func(
	originalWorkloadInfo WorkloadInfo, // the target workload
	compressedWorkloadInfo WorkloadInfo, // the compressed workload
	parameter Parameter, // the input parameters
	optimizer WhatIfOptimizer, // the what-if optimizer
) (Set[Index], error)

// IndexableColumnsSelectionAlgo is the interface for indexable columns selection algorithms.
type IndexableColumnsSelectionAlgo func(workloadInfo *WorkloadInfo) error

// WorkloadInfoCompressionAlgo is the interface for workload info compression algorithms.
type WorkloadInfoCompressionAlgo func(workloadInfo WorkloadInfo) WorkloadInfo

var (
	compressAlgorithms = map[string]WorkloadInfoCompressionAlgo{
		"none":       NoneWorkloadInfoCompress,
		"naive":      NaiveWorkloadInfoCompress,
		"clustering": ClusteringWorkloadInfoCompress,
	}

	findIndexableColsAlgorithms = map[string]IndexableColumnsSelectionAlgo{
		"simple": IndexableColumnsSelectionSimple,
	}

	selectIndexAlgorithms = map[string]IndexSelectionAlgo{
		"auto_admin": SelectIndexAAAlgo,
		"example":    SelectIndexExample,
		"genetic":    nil,
	}
)

type Parameter struct {
	MaximumIndexesToRecommend int
	//StorageBudgetInBytes          int
	//ConsiderTiFlashReplica        bool // whether to consider recommending TiFlash replica
	//ConsiderRemoveExistingIndexes bool // whether to consider removing existing indexes
}

func IndexAdvise(compressAlgo, indexableAlgo, selectionAlgo, dsn string, originalWorkloadInfo WorkloadInfo, param Parameter) error {
	Debugf("starting index advise with compress algorithm %s, indexable algorithm %s, index selection algorithm %s", compressAlgo, indexableAlgo, selectionAlgo)

	compress, ok := compressAlgorithms[compressAlgo]
	if !ok {
		return fmt.Errorf("compress algorithm %s not found", compressAlgo)
	}

	indexable, ok := findIndexableColsAlgorithms[indexableAlgo]
	if !ok {
		return fmt.Errorf("indexable algorithm %s not found", indexableAlgo)
	}

	selection, ok := selectIndexAlgorithms[selectionAlgo]
	if !ok {
		return fmt.Errorf("selection algorithm %s not found", selectionAlgo)
	}

	optimizer, err := NewTiDBWhatIfOptimizer(dsn)
	if err != nil {
		return err
	}

	compressedWorkloadInfo := compress(originalWorkloadInfo)
	Debugf("compressing workload info from %v SQLs to %v SQLs", originalWorkloadInfo.SQLs.Size(), compress(originalWorkloadInfo).SQLs.Size())

	must(indexable(&compressedWorkloadInfo))
	must(indexable(&originalWorkloadInfo))
	Debugf("finding %v indexable columns", compressedWorkloadInfo.IndexableColumns.Size())

	checkWorkloadInfo(compressedWorkloadInfo)
	checkWorkloadInfo(originalWorkloadInfo)
	recommendedIndexes, err := selection(originalWorkloadInfo, compressedWorkloadInfo, param, optimizer)
	must(err)

	PrintAdvisorResult(recommendedIndexes, originalWorkloadInfo, optimizer)
	return nil
}

func PrintAdvisorResult(indexes Set[Index], workload WorkloadInfo, optimizer WhatIfOptimizer) {
	fmt.Println("===================== index advisor result =====================")
	defer fmt.Println("===================== index advisor result =====================")
	indexList := indexes.ToList()
	sort.Slice(indexList, func(i, j int) bool {
		return indexList[i].Key() < indexList[j].Key()
	})
	for _, index := range indexList {
		fmt.Println(index.DDL() + ";")
	}

	sqls := workload.SQLs.ToList()
	var oriPlans, optPlans []Plan
	for _, sql := range sqls {
		p, err := optimizer.GetPlanCost(sql.Text)
		must(err)
		oriPlans = append(oriPlans, p)
	}
	for _, idx := range indexList {
		must(optimizer.CreateHypoIndex(idx))
	}
	for _, sql := range sqls {
		p, err := optimizer.GetPlanCost(sql.Text)
		must(err)
		optPlans = append(optPlans, p)
	}
	for _, idx := range indexList {
		must(optimizer.DropHypoIndex(idx))
	}

	type PlanDiff struct {
		SQL     SQL
		OriPlan Plan
		OptPlan Plan
	}
	var planDiffs []PlanDiff
	for i := range sqls {
		planDiffs = append(planDiffs, PlanDiff{
			SQL:     sqls[i],
			OriPlan: oriPlans[i],
			OptPlan: optPlans[i],
		})
	}
	sort.Slice(planDiffs, func(i, j int) bool {
		return planDiffs[i].OptPlan.Cost/planDiffs[i].OriPlan.Cost < planDiffs[j].OptPlan.Cost/planDiffs[j].OriPlan.Cost
	})

	for _, diff := range planDiffs {
		fmt.Println("-------------------------------------------------")
		if diff.SQL.Alias != "" {
			fmt.Printf("SQL: %s\n", diff.SQL.Alias)
		} else {
			fmt.Printf("SQL: %s\n", diff.SQL.Text)
		}
		ratio := diff.OptPlan.Cost / diff.OriPlan.Cost
		fmt.Printf("Cost Ratio: %.2f\n", diff.OptPlan.Cost/diff.OriPlan.Cost)
		if ratio < 0.8 {
			PrintPlan(diff.OriPlan)
			PrintPlan(diff.OptPlan)
		}
	}
}

func PrintPlan(p Plan) {
	for _, line := range p.Plan {
		fmt.Println(line)
	}
}
