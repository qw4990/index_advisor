package cmd

import (
	"fmt"
	"os"
	"path"
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
}

func NewExecWorkloadCmd() *cobra.Command {
	var opt execWorkloadCmdOpt
	cmd := &cobra.Command{
		Use:   "exec-workload",
		Short: "exec all queries in the specified workload",
		Long:  `exec all queries in the specified workload and collect their plans and execution times`,
		RunE: func(cmd *cobra.Command, args []string) error {
			info, err := utils.LoadWorkloadInfo(opt.schemaName, opt.workloadPath)
			if err != nil {
				return err
			}

			if opt.queries != "" {
				qs := strings.Split(opt.queries, ",")
				info.SQLs = utils.FilterBySQLAlias(info.SQLs, qs)
			}

			db, err := optimizer.NewTiDBWhatIfOptimizer(opt.dsn)
			utils.Must(err)
			utils.Must(db.Execute(`use ` + opt.schemaName))

			sqls := info.SQLs.ToList()
			sort.Slice(sqls, func(i, j int) bool {
				return sqls[i].Alias < sqls[j].Alias
			})

			savePath := path.Join(opt.workloadPath, "exec-workload-result-"+opt.prefix)

			execWorkload(db, info, savePath)
			return nil
		},
	}

	cmd.Flags().StringVar(&opt.dsn, "dsn", "root:@tcp(127.0.0.1:4000)/test", "dsn")
	cmd.Flags().StringVar(&opt.schemaName, "schema-name", "test", "the schema(database) name to run all queries on the workload")
	cmd.Flags().StringVar(&opt.workloadPath, "workload-info-path", "", "workload info path")
	cmd.Flags().StringVar(&opt.prefix, "prefix", "exec", "prefix")
	cmd.Flags().StringVar(&opt.queries, "queries", "", "queries to consider, e.g. 'q1, q2'")
	return cmd
}

func execWorkload(db optimizer.WhatIfOptimizer, info utils.WorkloadInfo, savePath string) {
	sqls := info.SQLs.ToList()
	sort.Slice(sqls, func(i, j int) bool {
		return sqls[i].Alias < sqls[j].Alias
	})

	os.MkdirAll(savePath, 0777)
	summaryContent := ""
	var totExecTime time.Duration
	for _, sql := range sqls {
		if sql.Type() != utils.SQLTypeSelect {
			continue
		}
		var execTimes []time.Duration
		var plans []utils.Plan
		for k := 0; k < 5; k++ {
			p, err := db.ExplainAnalyze(sql.Text)
			utils.Must(err)
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
		content += fmt.Sprintf("SQL:\n %s\n\n", sql.Text)
		for _, p := range plans {
			content += fmt.Sprintf("%v\n", p.Format())
		}
		utils.SaveContentTo(fmt.Sprintf("%v/%v.txt", savePath, sql.Alias), content)

		summaryContent += fmt.Sprintf("%v %v\n", sql.Alias, avgTime)
		fmt.Println(sql.Alias, avgTime)
	}
	fmt.Println("TotalExecutionTime:", totExecTime)
	summaryContent += fmt.Sprintf("TotalExecutionTime: %v\n", totExecTime)
	utils.SaveContentTo(fmt.Sprintf("%v/summary.txt", savePath), summaryContent)
}
