package main

import (
	"github.com/spf13/cobra"
)

var (
	rootCmd = &cobra.Command{
		Use:   "TiDB-index-advisor",
		Short: "TiDB index advisor",
		Long:  `TiDB index advisor`,
	}
)

type adviseCmdOpt struct {
	numIndexes             int
	storageBudgetInBytes   int
	considerTiFlashReplica bool

	dsn                  string
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
			info, err := LoadWorkloadInfo(opt.workloadPath)
			if err != nil {
				return err
			}
			_, err = IndexAdvise(opt.workloadCompressAlgo, opt.indexableColsAlgo, opt.indexSelectionAlgo, opt.dsn, info, Parameter{
				NumIndexesToRecommend:  opt.numIndexes,
				StorageBudgetInBytes:   opt.storageBudgetInBytes,
				ConsiderTiFlashReplica: opt.considerTiFlashReplica,
			})
			return err
		},
	}

	cmd.Flags().IntVar(&opt.numIndexes, "num-indexes", 10, "number of indexes to recommend, 0 means no limit")
	cmd.Flags().IntVar(&opt.storageBudgetInBytes, "storage-budget", 0, "storage budget in bytes, 0 means no budget")
	cmd.Flags().BoolVar(&opt.considerTiFlashReplica, "consider-tiflash-replica", false, "whether to consider tiflash replica")

	cmd.Flags().StringVar(&opt.dsn, "dsn", "mysql://root:@127.0.0.1:4000/test", "dsn")
	cmd.Flags().StringVar(&opt.workloadPath, "workload-info-path", "", "workload info path")
	cmd.Flags().StringVar(&opt.workloadCompressAlgo, "workload-compress-algo", "none", "workload compression algorithm")
	cmd.Flags().StringVar(&opt.indexableColsAlgo, "indexable-column-algo", "simple", "indexable column finding algorithm")
	cmd.Flags().StringVar(&opt.indexSelectionAlgo, "index-selection-algo", "auto_admin", "index selection algorithm")
	return cmd
}

func init() {
	cobra.OnInitialize()
	rootCmd.AddCommand(newAdviseCmd())
}

func main() {
	rootCmd.Execute()
}
