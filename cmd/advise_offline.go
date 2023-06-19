package cmd

import (
	"fmt"
	"os"
	"path"
	"sort"
	"strings"

	"github.com/qw4990/index_advisor/advisor"
	"github.com/qw4990/index_advisor/optimizer"
	"github.com/qw4990/index_advisor/utils"
	wk "github.com/qw4990/index_advisor/workload"
	"github.com/spf13/cobra"
)

type adviseOfflineCmdOpt struct {
	maxNumIndexes int
	maxIndexWidth int

	dsn          string
	schemaName   string
	workloadPath string
	queries      string
}

func NewAdviseOfflineCmd() *cobra.Command {
	var opt adviseOfflineCmdOpt
	cmd := &cobra.Command{
		Use:   "advise-offline",
		Short: "advise some indexes for the specified workload",
		Long:  `advise some indexes for the specified workload`,
		RunE: func(cmd *cobra.Command, args []string) error {
			info, err := wk.LoadWorkloadInfo(opt.schemaName, opt.workloadPath)
			if err != nil {
				return err
			}

			if opt.queries != "" {
				qs := strings.Split(opt.queries, ",")
				info.SQLs = wk.FilterBySQLAlias(info.SQLs, qs)
			}

			db, err := optimizer.NewTiDBWhatIfOptimizer(opt.dsn)
			if err != nil {
				return err
			}

			savePath := path.Join(opt.workloadPath, "advise-result")
			indexes, err := advisor.IndexAdvise(db, info, &advisor.Parameter{
				MaxNumberIndexes: opt.maxNumIndexes,
				MaxIndexWidth:    opt.maxIndexWidth,
			})
			utils.Must(err)
			PrintAndSaveAdviseResult(savePath, indexes, info, db)
			return err
		},
	}

	cmd.Flags().IntVar(&opt.maxNumIndexes, "max-num-indexes", 10, "max number of indexes to recommend, 0 means no limit")
	cmd.Flags().IntVar(&opt.maxNumIndexes, "max-index-width", 3, "the max number of columns in recommended indexes")

	cmd.Flags().StringVar(&opt.dsn, "dsn", "root:@tcp(127.0.0.1:4000)/test", "dsn")
	cmd.Flags().StringVar(&opt.schemaName, "schema-name", "test", "the schema(database) name to run all queries on the workload")
	cmd.Flags().StringVar(&opt.workloadPath, "workload-info-path", "", "workload info path")
	cmd.Flags().StringVar(&opt.queries, "queries", "", "queries to consider, e.g. 'q1, q2'")
	return cmd
}

// PrintAndSaveAdviseResult prints and saves the index advisor result.
func PrintAndSaveAdviseResult(savePath string, indexes utils.Set[wk.Index], workload wk.WorkloadInfo, optimizer optimizer.WhatIfOptimizer) {
	fmt.Println("===================== index advisor result =====================")
	defer fmt.Println("===================== index advisor result =====================")
	if savePath != "" {
		os.MkdirAll(savePath, 0777)
	}
	indexList := indexes.ToList()
	sort.Slice(indexList, func(i, j int) bool {
		return indexList[i].Key() < indexList[j].Key()
	})
	ddlContent := ""
	for _, index := range indexList {
		ddlContent += index.DDL() + ";\n"
	}
	fmt.Println(ddlContent)
	if savePath != "" {
		utils.SaveContentTo(path.Join(savePath, "ddl.sql"), ddlContent)
	}

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
		if savePath != "" {
			utils.SaveContentTo(ppath, content)
		}
		oriTotCost += diff.OriPlan.PlanCost()
		optTotCost += diff.OptPlan.PlanCost()

		if diff.SQL.Alias != "" {
			summary := fmt.Sprintf("Cost Ratio for %v: %.2f\n", diff.SQL.Alias, diff.OptPlan.PlanCost()/diff.OriPlan.PlanCost())
			fmt.Printf(summary)
			summaryContent += summary
		}
	}
	fmt.Printf("total cost ratio: %.2E/%.2E=%.2f\n", optTotCost, oriTotCost, optTotCost/oriTotCost)
	if savePath != "" {
		utils.SaveContentTo(path.Join(savePath, "summary.txt"), summaryContent)
	}
}
