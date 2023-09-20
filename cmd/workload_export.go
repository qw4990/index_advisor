package cmd

import (
	"github.com/qw4990/index_advisor/utils"
	"github.com/spf13/cobra"
)

type workloadExportCmdOpt struct {
	dsn      string
	output   string
	logLevel string

	querySchemas []string
}

func NewWorkloadExportCmd() *cobra.Command {
	var opt workloadExportCmdOpt
	cmd := &cobra.Command{
		Use:   "workload-export",
		Short: "export workload information (queries, table schema, table statistics) from your TiDB cluster, use `index_advisor workload-export --help` to see more details`",
		Long: `export workload information (queries, table schema, table statistics) from your TiDB cluster.
How it work:
1. connect to your TiDB cluster through the DSN
2. read all queries from the 'STATEMENT_SUMMARY' system table
3. read all table schema from the 'INFORMATION_SCHEMA' database
4. read all statistics from the 'mysql.stats_xxx' system tables
5. store all data into the specified output directory
`,
		RunE: func(cmd *cobra.Command, args []string) error {
			utils.SetLogLevel(opt.logLevel)
			return nil
		},
	}

	cmd.Flags().StringVar(&opt.dsn, "dsn", "root:@tcp(127.0.0.1:4000)/test", "dsn")
	cmd.Flags().StringVar(&opt.output, "output", "", "output directory to save the result")
	cmd.Flags().StringVar(&opt.logLevel, "log-level", "info", "log level, one of 'debug', 'info', 'warning', 'error'")
	cmd.Flags().StringSliceVar(&opt.querySchemas, "query-schemas", []string{}, "a list of schema(database), e.g. 'test1, test2', queries that are running under these schemas will be considered")
	return cmd
}
