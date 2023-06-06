package main

import (
	"fmt"
	"os"
	"path"
	"path/filepath"
	"sort"
	"time"

	"github.com/go-sql-driver/mysql"
	"github.com/spf13/cobra"
)

var (
	rootCmd = &cobra.Command{
		Use:   "TiDB-index-advisor",
		Short: "TiDB index advisor",
		Long:  `TiDB index advisor`,
	}
)

type runWorkloadCmdOpt struct {
	dsn          string
	schemaName   string
	workloadPath string
}

func newRunWorkloadCmd() *cobra.Command {
	var opt runWorkloadCmdOpt

	cmd := &cobra.Command{
		Use:   "run-workload",
		Short: "run workload",
		Long:  `run workload`,
		RunE: func(cmd *cobra.Command, args []string) error {
			info, err := LoadWorkloadInfo(opt.schemaName, opt.workloadPath)
			if err != nil {
				return err
			}

			db, err := NewTiDBWhatIfOptimizer(opt.dsn)
			must(err)
			must(db.Execute(`use ` + opt.schemaName))

			sqls := info.SQLs.ToList()
			sort.Slice(sqls, func(i, j int) bool {
				return sqls[i].Alias < sqls[j].Alias
			})

			savePath := path.Join(opt.workloadPath, "result")
			for _, sql := range sqls {
				if sql.Type() != SQLTypeSelect {
					continue
				}
				var execTimes []time.Duration
				var plans []Plan
				for k := 0; k < 5; k++ {
					p, err := db.ExplainAnalyzeQuery(sql.Text)
					must(err)
					plans = append(plans, p)
					execTimes = append(execTimes, p.ExecTime())
				}
				sort.Slice(execTimes, func(i, j int) bool {
					return execTimes[i] < execTimes[j]
				})
				avgTime := (execTimes[1] + execTimes[2] + execTimes[3]) / 3

				content := fmt.Sprintf("Alias: %s\n", sql.Alias)
				content += fmt.Sprintf("AvgTime: %v\n", avgTime)
				content += fmt.Sprintf("ExecTimes: %v\n", execTimes)
				content += fmt.Sprintf("SQL:\n %s\n\n", sql.Text)
				for _, p := range plans {
					content += fmt.Sprintf("%v\n", FormatPlan(p))
				}
				saveContentTo(fmt.Sprintf("%v/exec_%v.txt", savePath, sql.Alias), content)
				fmt.Println(sql.Alias, avgTime)
			}
			return nil
		},
	}

	cmd.Flags().StringVar(&opt.dsn, "dsn", "root:@tcp(127.0.0.1:4000)/test", "dsn")
	cmd.Flags().StringVar(&opt.schemaName, "schema-name", "test", "the schema(database) name to run all queries on the workload")
	cmd.Flags().StringVar(&opt.workloadPath, "workload-info-path", "", "workload info path")
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
		Short: "load workload into your cluster",
		Long:  `load workload into your cluster`,
		RunE: func(cmd *cobra.Command, args []string) error {
			// create a connection
			db, err := NewTiDBWhatIfOptimizer(opt.dsn)
			must(err)

			// create the corresponding database
			must(db.Execute(`create database if not exists ` + opt.schemaName))
			must(db.Execute(`use ` + opt.schemaName))

			// create tables
			schemaSQLPath := path.Join(opt.workloadPath, "schema.sql")
			schemaSQLs, err := ParseRawSQLsFromFile(schemaSQLPath)
			must(err)
			for _, stmt := range schemaSQLs {
				must(db.Execute(stmt))
			}

			// load statistics
			statsFiles, err := os.ReadDir(path.Join(opt.workloadPath, "stats"))
			must(err)
			for _, statsFile := range statsFiles {
				statsPath := path.Join(opt.workloadPath, "stats", statsFile.Name())
				absStatsPath, err := filepath.Abs(statsPath)
				must(err, statsPath)
				mysql.RegisterLocalFile(absStatsPath)
				loadStatsSQL := fmt.Sprintf("load stats '%s'", absStatsPath)
				must(db.Execute(loadStatsSQL))
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
	//storageBudgetInBytes   int
	//considerTiFlashReplica bool

	dsn                  string
	schemaName           string
	workloadPath         string
	workloadCompressAlgo string
	indexableColsAlgo    string
	indexSelectionAlgo   string
}

func newAdviseCmd() *cobra.Command {
	var opt adviseCmdOpt
	var logLevel string
	cmd := &cobra.Command{
		Use:   "advise",
		Short: "advise",
		Long:  `advise`,
		RunE: func(cmd *cobra.Command, args []string) error {
			updateLogLevel(logLevel)
			info, err := LoadWorkloadInfo(opt.schemaName, opt.workloadPath)
			if err != nil {
				return err
			}
			savePath := path.Join(opt.workloadPath, "result")
			return IndexAdvise(opt.workloadCompressAlgo, opt.indexableColsAlgo, opt.indexSelectionAlgo, opt.dsn, savePath, info, Parameter{
				MaximumIndexesToRecommend: opt.maxNumIndexes,
				//StorageBudgetInBytes:      opt.storageBudgetInBytes,
				//ConsiderTiFlashReplica:    opt.considerTiFlashReplica,
			})
		},
	}

	cmd.Flags().IntVar(&opt.maxNumIndexes, "max-num-indexes", 10, "max number of indexes to recommend, 0 means no limit")
	//cmd.Flags().IntVar(&opt.storageBudgetInBytes, "storage-budget", 0, "storage budget in bytes, 0 means no budget")
	//cmd.Flags().BoolVar(&opt.considerTiFlashReplica, "consider-tiflash-replica", false, "whether to consider tiflash replica")

	cmd.Flags().StringVar(&opt.dsn, "dsn", "root:@tcp(127.0.0.1:4000)/test", "dsn")
	cmd.Flags().StringVar(&opt.schemaName, "schema-name", "test", "the schema(database) name to run all queries on the workload")
	cmd.Flags().StringVar(&opt.workloadPath, "workload-info-path", "", "workload info path")
	cmd.Flags().StringVar(&opt.workloadCompressAlgo, "workload-compress-algo", "none", "workload compression algorithm")
	cmd.Flags().StringVar(&opt.indexableColsAlgo, "indexable-column-algo", "simple", "indexable column finding algorithm")
	cmd.Flags().StringVar(&opt.indexSelectionAlgo, "index-selection-algo", "auto_admin", "index selection algorithm")

	cmd.Flags().StringVar(&logLevel, "log-level", "debug", "log level")
	return cmd
}

func init() {
	cobra.OnInitialize()
	rootCmd.AddCommand(newAdviseCmd())
	rootCmd.AddCommand(newLoadWorkloadCmd())
	rootCmd.AddCommand(newRunWorkloadCmd())
}

func main() {
	rootCmd.Execute()
}
