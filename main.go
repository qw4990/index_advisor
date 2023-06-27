package main

import (
	"github.com/qw4990/index_advisor/cmd"
	"github.com/spf13/cobra"
)

var (
	rootCmd = &cobra.Command{
		Use:   "TiDB-index-advisor",
		Short: "TiDB index advisor",
		Long:  `TiDB index advisor recommends you the best indexes for your workload`,
	}
)

func init() {
	cobra.OnInitialize()
	//rootCmd.AddCommand(cmd.NewAdviseOnlineCmd())
	rootCmd.AddCommand(cmd.NewAdviseOfflineCmd())
	rootCmd.AddCommand(cmd.NewEvaluateCmd())
}

func main() {
	rootCmd.Execute()
}
