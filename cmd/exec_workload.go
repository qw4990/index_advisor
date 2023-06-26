package cmd

import (
	"fmt"
	"os"
	"sort"
	"strings"
	"time"

	"github.com/qw4990/index_advisor/optimizer"
	"github.com/qw4990/index_advisor/utils"
	"github.com/spf13/cobra"
)

type execWorkloadCmdOpt struct {
	dsn          string
	schemaName   string
	workloadPath string
	prefix       string
	queries      string
	output       string
}

func NewExecWorkloadCmd() *cobra.Command {
	var opt execWorkloadCmdOpt
	cmd := &cobra.Command{
		Use:   "exec-workload",
		Short: "exec all queries in the specified workload",
		Long:  `exec all queries in the specified workload and collect their plans and execution times`,
		RunE: func(cmd *cobra.Command, args []string) error {
			queries, err := utils.LoadQueries(opt.schemaName, opt.workloadPath)
			if err != nil {
				return err
			}

			if opt.queries != "" {
				qs := strings.Split(opt.queries, ",")
				queries = utils.FilterBySQLAlias(queries, qs)
			}

			db, err := optimizer.NewTiDBWhatIfOptimizer(opt.dsn)
			if err != nil {
				return err
			}
			if err := db.Execute(`use ` + opt.schemaName); err != nil {
				return err
			}

			sqls := queries.ToList()
			sort.Slice(sqls, func(i, j int) bool {
				return sqls[i].Alias < sqls[j].Alias
			})

			return execWorkload(db, queries, opt.output)
		},
	}

	cmd.Flags().StringVar(&opt.dsn, "dsn", "root:@tcp(127.0.0.1:4000)/test", "dsn")
	cmd.Flags().StringVar(&opt.schemaName, "schema-name", "test", "the schema(database) name to run all queries on the workload")
	cmd.Flags().StringVar(&opt.workloadPath, "workload-info-path", "", "workload info path")
	cmd.Flags().StringVar(&opt.prefix, "prefix", "exec", "prefix")
	cmd.Flags().StringVar(&opt.queries, "queries", "", "queries to consider, e.g. 'q1, q2'")
	cmd.Flags().StringVar(&opt.output, "output", "", "output directory to save the result")
	return cmd
}

func execWorkload(db optimizer.WhatIfOptimizer, queries utils.Set[utils.Query], savePath string) error {
	queryList := queries.ToList()
	sort.Slice(queryList, func(i, j int) bool {
		return queryList[i].Alias < queryList[j].Alias
	})

	os.MkdirAll(savePath, 0777)
	summaryContent := ""
	var totExecTime time.Duration
	for _, sql := range queryList {
		var execTimes []time.Duration
		var plans []utils.Plan
		for k := 0; k < 5; k++ {
			p, err := db.ExplainAnalyze(sql.Text)
			if err != nil {
				return err
			}
			plans = append(plans, p)
			execTimes = append(execTimes, p.ExecTime())
		}
		sort.Slice(execTimes, func(i, j int) bool {
			return execTimes[i] < execTimes[j]
		})
		avgTime := (execTimes[1] + execTimes[2] + execTimes[3]) / 3
		totExecTime += avgTime

		content := fmt.Sprintf("Alias: %s\n", sql.Alias)
		content += fmt.Sprintf("AvgTime: %v\n", avgTime)
		content += fmt.Sprintf("ExecTimes: %v\n", execTimes)
		content += fmt.Sprintf("Query:\n %s\n\n", sql.Text)
		for _, p := range plans {
			content += fmt.Sprintf("%v\n", p.Format())
		}
		utils.SaveContentTo(fmt.Sprintf("%v/%v.txt", savePath, sql.Alias), content)

		summaryContent += fmt.Sprintf("%v %v\n", sql.Alias, avgTime)
		fmt.Println(sql.Alias, avgTime)
	}
	fmt.Println("TotalExecutionTime:", totExecTime)
	summaryContent += fmt.Sprintf("TotalExecutionTime: %v\n", totExecTime)
	return utils.SaveContentTo(fmt.Sprintf("%v/summary.txt", savePath), summaryContent)
}
