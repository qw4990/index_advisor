package cmd

import (
	"path"
	"strings"

	"github.com/qw4990/index_advisor/advisor"
	"github.com/qw4990/index_advisor/utils"
	wk "github.com/qw4990/index_advisor/workload"
	"github.com/spf13/cobra"
)

type adviseOfflineCmdOpt struct {
	maxNumIndexes int

	dsn          string
	schemaName   string
	workloadPath string
	queries      string
}

func NewAdviseOfflineCmd() *cobra.Command {
	var opt adviseOfflineCmdOpt
	var logLevel string
	cmd := &cobra.Command{
		Use:   "advise-offline",
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
