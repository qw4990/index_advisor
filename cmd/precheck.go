package cmd

import (
	"errors"

	"github.com/qw4990/index_advisor/optimizer"
	"github.com/spf13/cobra"
)

func NewPreCheckCmd() *cobra.Command {
	var dsn string
	cmd := &cobra.Command{
		Use:   "precheck",
		Short: "check whether this cluster is suitable for online-mode",
		Long:  `check whether this cluster is suitable for index advisor online-mode`,
		RunE: func(cmd *cobra.Command, args []string) error {
			db, err := optimizer.NewTiDBWhatIfOptimizer(dsn)
			if err != nil {
				return err
			}

			if !supportHypoIndex(db) {
				return errors.New("your TiDB version does not support hypothetical index feature, which is required by Index Advisor")
			}
			if redactLogEnabled(db) {
				return errors.New("redact log is enabled, the Advisor probably cannot get the full SQL text")
			}
			return nil
		},
	}

	cmd.Flags().StringVar(&dsn, "dsn", "root:@tcp(127.0.0.1:4000)/test", "the DSN of the TiDB cluster")
	return cmd
}
