package advisor

import (
	"fmt"
	"github.com/qw4990/index_advisor/optimizer"
	"github.com/qw4990/index_advisor/utils"
	wk "github.com/qw4990/index_advisor/workload"
	"os"
	"path"
	"sort"
)

// IndexSelectionAlgo is the interface for index selection algorithms.
type IndexSelectionAlgo func(
	workloadInfo wk.WorkloadInfo, // the target workload
	parameter Parameter, // the input parameters
	optimizer optimizer.WhatIfOptimizer, // the what-if optimizer
) (utils.Set[wk.Index], error)

// IndexableColumnsSelectionAlgo is the interface for indexable columns selection algorithms.
type IndexableColumnsSelectionAlgo func(workloadInfo *wk.WorkloadInfo) error

// WorkloadInfoCompressionAlgo is the interface for workload info compression algorithms.
type WorkloadInfoCompressionAlgo func(workloadInfo wk.WorkloadInfo) wk.WorkloadInfo

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
}

// IndexAdvise is the entry point of index advisor.
func IndexAdvise(compressAlgo, indexableAlgo, selectionAlgo, dsn, savePath string, originalWorkloadInfo wk.WorkloadInfo, param Parameter) error {
	utils.Debugf("starting index advise with compress algorithm %s, indexable algorithm %s, index selection algorithm %s", compressAlgo, indexableAlgo, selectionAlgo)

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

	optimizer, err := optimizer.NewTiDBWhatIfOptimizer(dsn)
	if err != nil {
		return err
	}

	compressedWorkloadInfo := compress(originalWorkloadInfo)
	if compressAlgo != "none" {
		utils.Debugf("compressing workload info from %v SQLs to %v SQLs", originalWorkloadInfo.SQLs.Size(), compress(originalWorkloadInfo).SQLs.Size())
	}

	utils.Must(indexable(&compressedWorkloadInfo))
	utils.Debugf("finding %v indexable columns", compressedWorkloadInfo.IndexableColumns.Size())

	checkWorkloadInfo(compressedWorkloadInfo)
	recommendedIndexes, err := selection(compressedWorkloadInfo, param, optimizer)
	utils.Must(err)

	PrintAndSaveAdviseResult(savePath, recommendedIndexes, originalWorkloadInfo, optimizer)
	return nil
}

// PrintAndSaveAdviseResult prints and saves the index advisor result.
func PrintAndSaveAdviseResult(savePath string, indexes utils.Set[wk.Index], workload wk.WorkloadInfo, optimizer optimizer.WhatIfOptimizer) {
	fmt.Println("===================== index advisor result =====================")
	defer fmt.Println("===================== index advisor result =====================")
	os.MkdirAll(savePath, 0777)
	indexList := indexes.ToList()
	sort.Slice(indexList, func(i, j int) bool {
		return indexList[i].Key() < indexList[j].Key()
	})
	ddlContent := ""
	for _, index := range indexList {
		ddlContent += index.DDL() + ";\n"
	}
	fmt.Println(ddlContent)
	utils.SaveContentTo(path.Join(savePath, "ddl.sql"), ddlContent)

	sqls := workload.SQLs.ToList()
	var oriPlans, optPlans []wk.Plan
	for _, sql := range sqls {
		p, err := optimizer.Explain(sql.Text)
		utils.Must(err)
		oriPlans = append(oriPlans, p)
	}
	for _, idx := range indexList {
		utils.Must(optimizer.CreateHypoIndex(idx))
	}
	for _, sql := range sqls {
		p, err := optimizer.Explain(sql.Text)
		utils.Must(err)
		optPlans = append(optPlans, p)
	}
	for _, idx := range indexList {
		utils.Must(optimizer.DropHypoIndex(idx))
	}

	type PlanDiff struct {
		SQL     wk.SQL
		OriPlan wk.Plan
		OptPlan wk.Plan
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
	var summaryContent string
	for i, diff := range planDiffs {
		content := ""
		content += fmt.Sprintf("Alias: %s\n", diff.SQL.Alias)
		content += fmt.Sprintf("SQL: \n%s\n\n", diff.SQL.Text)
		content += fmt.Sprintf("Original Cost: %.2E\n", diff.OriPlan.PlanCost())
		content += fmt.Sprintf("Optimized Cost: %.2E\n", diff.OptPlan.PlanCost())
		content += fmt.Sprintf("Cost Ratio: %.2f\n", diff.OptPlan.PlanCost()/diff.OriPlan.PlanCost())
		content += "\n\n------------------ original plan ------------------\n"
		content += diff.OriPlan.Format()
		content += "\n\n------------------ optimized plan -----------------\n"
		content += diff.OptPlan.Format()
		var ppath string
		if diff.SQL.Alias != "" {
			ppath = path.Join(savePath, fmt.Sprintf("%s.txt", diff.SQL.Alias))
		} else {
			ppath = path.Join(savePath, fmt.Sprintf("q%v.txt", i))
		}
		utils.SaveContentTo(ppath, content)
		oriTotCost += diff.OriPlan.PlanCost()
		optTotCost += diff.OptPlan.PlanCost()

		if diff.SQL.Alias != "" {
			summary := fmt.Sprintf("Cost Ratio for %v: %.2f\n", diff.SQL.Alias, diff.OptPlan.PlanCost()/diff.OriPlan.PlanCost())
			fmt.Printf(summary)
			summaryContent += summary
		}
	}
	fmt.Printf("total cost ratio: %.2E/%.2E=%.2f\n", optTotCost, oriTotCost, optTotCost/oriTotCost)
	utils.SaveContentTo(path.Join(savePath, "summary.txt"), summaryContent)
}
