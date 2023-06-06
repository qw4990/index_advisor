package main

import (
	"fmt"
	"os"
	"path"
	"sort"
	"strings"
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
		"genetic":    nil,
	}
)

type Parameter struct {
	MaximumIndexesToRecommend int
	//StorageBudgetInBytes          int
	//ConsiderTiFlashReplica        bool // whether to consider recommending TiFlash replica
	//ConsiderRemoveExistingIndexes bool // whether to consider removing existing indexes
}

func IndexAdvise(compressAlgo, indexableAlgo, selectionAlgo, dsn, savePath string, originalWorkloadInfo WorkloadInfo, param Parameter) error {
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

	SaveResult(savePath, recommendedIndexes, originalWorkloadInfo, optimizer)
	return nil
}

func SaveResult(savePath string, indexes Set[Index], workload WorkloadInfo, optimizer WhatIfOptimizer) {
	fmt.Println("===================== index advisor result =====================")
	defer fmt.Println("===================== index advisor result =====================")
	indexList := indexes.ToList()
	sort.Slice(indexList, func(i, j int) bool {
		return indexList[i].Key() < indexList[j].Key()
	})
	ddlContent := ""
	for _, index := range indexList {
		ddlContent += index.DDL() + ";\n"
	}
	fmt.Println(ddlContent)
	saveContentTo(path.Join(savePath, "ddl.sql"), ddlContent)

	sqls := workload.SQLs.ToList()
	var oriPlans, optPlans []Plan
	for _, sql := range sqls {
		p, err := optimizer.Explain(sql.Text)
		must(err)
		oriPlans = append(oriPlans, p)
	}
	for _, idx := range indexList {
		must(optimizer.CreateHypoIndex(idx))
	}
	for _, sql := range sqls {
		p, err := optimizer.Explain(sql.Text)
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
		return planDiffs[i].OptPlan.PlanCost()/planDiffs[i].OriPlan.PlanCost() < planDiffs[j].OptPlan.PlanCost()/planDiffs[j].OriPlan.PlanCost()
	})

	var oriTotCost, optTotCost float64
	for i, diff := range planDiffs {
		content := ""
		content += fmt.Sprintf("Alias: %s\n", diff.SQL.Alias)
		content += fmt.Sprintf("SQL: \n%s\n\n", diff.SQL.Text)
		content += fmt.Sprintf("Cost Ratio: %.2f\n", diff.OptPlan.PlanCost()/diff.OriPlan.PlanCost())
		content += "\n\n------------------ original plan ------------------\n"
		content += FormatPlan(diff.OriPlan)
		content += "\n\n------------------ optimized plan -----------------\n"
		content += FormatPlan(diff.OptPlan)
		var ppath string
		if diff.SQL.Alias != "" {
			ppath = path.Join(savePath, fmt.Sprintf("%s.txt", diff.SQL.Alias))
		} else {
			ppath = path.Join(savePath, fmt.Sprintf("q%v.txt", i))
		}
		saveContentTo(ppath, content)
		oriTotCost += diff.OriPlan.PlanCost()
		optTotCost += diff.OptPlan.PlanCost()

		if diff.SQL.Alias != "" {
			fmt.Printf("Cost Ratio for %v: %.2f\n", diff.SQL.Alias, diff.OptPlan.PlanCost()/diff.OriPlan.PlanCost())
		}
	}
	fmt.Printf("total cost ratio: %.2E/%.2E=%.2f\n", optTotCost, oriTotCost, optTotCost/oriTotCost)
}

func saveContentTo(fpath, content string) {
	must(os.WriteFile(fpath, []byte(content), 0644))
}

func FormatPlan(p Plan) string {
	var lines []string
	for _, line := range p.Plan {
		lines = append(lines, strings.Join(line, "\t"))
	}
	return strings.Join(lines, "\n")
}
