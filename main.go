package main

import (
	"fmt"
	"os"
	"path"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"github.com/go-sql-driver/mysql"
	"github.com/qw4990/index_advisor/advisor"
	"github.com/qw4990/index_advisor/optimizer"
	"github.com/qw4990/index_advisor/utils"
	wk "github.com/qw4990/index_advisor/workload"
	"github.com/spf13/cobra"
)

var (
	rootCmd = &cobra.Command{
		Use:   "TiDB-index-advisor",
		Short: "TiDB index advisor",
		Long:  `TiDB index advisor`,
	}
)

type execWorkloadCmdOpt struct {
	dsn          string
	schemaName   string
	workloadPath string
	prefix       string
	queries      string
}

func newExecWorkloadCmd() *cobra.Command {
	var opt execWorkloadCmdOpt

	cmd := &cobra.Command{
		Use:   "exec-workload",
		Short: "exec all queries in the specified workload",
		Long:  `exec all queries in the specified workload and collect their plans and execution times`,
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
			utils.Must(err)
			utils.Must(db.Execute(`use ` + opt.schemaName))

			sqls := info.SQLs.ToList()
			sort.Slice(sqls, func(i, j int) bool {
				return sqls[i].Alias < sqls[j].Alias
			})

			savePath := path.Join(opt.workloadPath, "exec-workload-result-"+opt.prefix)
			os.MkdirAll(savePath, 0777)
			summaryContent := ""
			var totExecTime time.Duration
			for _, sql := range sqls {
				if sql.Type() != wk.SQLTypeSelect {
					continue
				}
				var execTimes []time.Duration
				var plans []wk.Plan
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

type loadWorkloadCmdOpt struct {
	dsn          string
	schemaName   string
	workloadPath string
}

func newLoadWorkloadCmd() *cobra.Command {
	var opt loadWorkloadCmdOpt
	cmd := &cobra.Command{
		Use:   "load-workload",
		Short: "load tables and related statistics of the specified workload into your cluster",
		Long:  `load tables and related statistics of the specified workload into your cluster`,
		RunE: func(cmd *cobra.Command, args []string) error {
			// create a connection
			db, err := optimizer.NewTiDBWhatIfOptimizer(opt.dsn)
			utils.Must(err)

			// create the corresponding database
			utils.Must(db.Execute(`create database if not exists ` + opt.schemaName))
			utils.Must(db.Execute(`use ` + opt.schemaName))

			// create tables
			schemaSQLPath := path.Join(opt.workloadPath, "schema.sql")
			schemaSQLs, err := utils.ParseRawSQLsFromFile(schemaSQLPath)
			utils.Must(err)
			for _, stmt := range schemaSQLs {
				utils.Must(db.Execute(stmt))
			}

			// load statistics
			statsFiles, err := os.ReadDir(path.Join(opt.workloadPath, "stats"))
			utils.Must(err)
			for _, statsFile := range statsFiles {
				statsPath := path.Join(opt.workloadPath, "stats", statsFile.Name())
				absStatsPath, err := filepath.Abs(statsPath)
				utils.Must(err, statsPath)
				mysql.RegisterLocalFile(absStatsPath)
				loadStatsSQL := fmt.Sprintf("load stats '%s'", absStatsPath)
				utils.Must(db.Execute(loadStatsSQL))
			}
			return nil
		},
	}

	cmd.Flags().StringVar(&opt.dsn, "dsn", "root:@tcp(127.0.0.1:4000)/test", "dsn")
	cmd.Flags().StringVar(&opt.schemaName, "schema-name", "test", "the schema(database) name to run all queries on the workload")
	cmd.Flags().StringVar(&opt.workloadPath, "workload-info-path", "", "workload info path")
	return cmd
}

type adviseCmdOpt struct {
	maxNumIndexes int

	dsn          string
	schemaName   string
	workloadPath string
	queries      string
}

func newAdviseCmd() *cobra.Command {
	var opt adviseCmdOpt
	var logLevel string
	cmd := &cobra.Command{
		Use:   "advise",
		Short: "advise some indexes for the specified workload",
		Long:  `advise some indexes for the specified workload`,
		RunE: func(cmd *cobra.Command, args []string) error {
			utils.SetLogLevel(logLevel)
			info, err := wk.LoadWorkloadInfo(opt.schemaName, opt.workloadPath)
			if err != nil {
				return err
			}

			if opt.queries != "" {
				qs := strings.Split(opt.queries, ",")
				info.SQLs = wk.FilterBySQLAlias(info.SQLs, qs)
			}

			savePath := path.Join(opt.workloadPath, "advise-result")
			return advisor.IndexAdvise("none", "simple", "auto_admin", opt.dsn, savePath, info,
				advisor.Parameter{MaximumIndexesToRecommend: opt.maxNumIndexes})
		},
	}

	cmd.Flags().IntVar(&opt.maxNumIndexes, "max-num-indexes", 10, "max number of indexes to recommend, 0 means no limit")

	cmd.Flags().StringVar(&opt.dsn, "dsn", "root:@tcp(127.0.0.1:4000)/test", "dsn")
	cmd.Flags().StringVar(&opt.schemaName, "schema-name", "test", "the schema(database) name to run all queries on the workload")
	cmd.Flags().StringVar(&opt.workloadPath, "workload-info-path", "", "workload info path")
	cmd.Flags().StringVar(&opt.queries, "queries", "", "queries to consider, e.g. 'q1, q2'")

	cmd.Flags().StringVar(&logLevel, "log-level", "debug", "log level")
	return cmd
}

func init() {
	cobra.OnInitialize()
	rootCmd.AddCommand(newAdviseCmd())
	rootCmd.AddCommand(newLoadWorkloadCmd())
	rootCmd.AddCommand(newExecWorkloadCmd())
}

func main() {
	rootCmd.Execute()
}
