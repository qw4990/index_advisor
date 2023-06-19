package cmd

import (
	"github.com/qw4990/index_advisor/optimizer"
	"path"
	"strings"

	"github.com/qw4990/index_advisor/advisor"
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
			return advisor.IndexAdvise(db, savePath, info, advisor.Parameter{
				MaxNumberIndexes: opt.maxNumIndexes,
				MaxIndexWidth:    opt.maxIndexWidth,
			})
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
