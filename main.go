package main

import (
	"fmt"
	"os"
	"path"
	"path/filepath"

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
	numIndexes             int
	storageBudgetInBytes   int
	considerTiFlashReplica bool

	dsn                  string
	schemaName           string
	workloadPath         string
	workloadCompressAlgo string
	indexableColsAlgo    string
	indexSelectionAlgo   string
}

func newAdviseCmd() *cobra.Command {
	var opt adviseCmdOpt
	cmd := &cobra.Command{
		Use:   "advise",
		Short: "advise",
		Long:  `advise`,
		RunE: func(cmd *cobra.Command, args []string) error {
			info, err := LoadWorkloadInfo(opt.schemaName, opt.workloadPath)
			if err != nil {
				return err
			}
			_, err = IndexAdvise(opt.workloadCompressAlgo, opt.indexableColsAlgo, opt.indexSelectionAlgo, opt.dsn, info, Parameter{
				MaximumIndexesToRecommend: opt.numIndexes,
				StorageBudgetInBytes:      opt.storageBudgetInBytes,
				ConsiderTiFlashReplica:    opt.considerTiFlashReplica,
			})
			return err
		},
	}

	cmd.Flags().IntVar(&opt.numIndexes, "num-indexes", 10, "number of indexes to recommend, 0 means no limit")
	cmd.Flags().IntVar(&opt.storageBudgetInBytes, "storage-budget", 0, "storage budget in bytes, 0 means no budget")
	cmd.Flags().BoolVar(&opt.considerTiFlashReplica, "consider-tiflash-replica", false, "whether to consider tiflash replica")

	cmd.Flags().StringVar(&opt.dsn, "dsn", "root:@tcp(127.0.0.1:4000)/test", "dsn")
	cmd.Flags().StringVar(&opt.schemaName, "schema-name", "test", "the schema(database) name to run all queries on the workload")
	cmd.Flags().StringVar(&opt.workloadPath, "workload-info-path", "", "workload info path")
	cmd.Flags().StringVar(&opt.workloadCompressAlgo, "workload-compress-algo", "none", "workload compression algorithm")
	cmd.Flags().StringVar(&opt.indexableColsAlgo, "indexable-column-algo", "simple", "indexable column finding algorithm")
	cmd.Flags().StringVar(&opt.indexSelectionAlgo, "index-selection-algo", "auto_admin", "index selection algorithm")
	return cmd
}

func init() {
	cobra.OnInitialize()
	rootCmd.AddCommand(newAdviseCmd())
	rootCmd.AddCommand(newLoadWorkloadCmd())
}

func main() {
	rootCmd.Execute()
}
