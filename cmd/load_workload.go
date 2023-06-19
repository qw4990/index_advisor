package cmd

import (
	"fmt"
	"os"
	"path"
	"path/filepath"

	"github.com/go-sql-driver/mysql"
	"github.com/qw4990/index_advisor/optimizer"
	"github.com/qw4990/index_advisor/utils"
	"github.com/spf13/cobra"
)

type loadWorkloadCmdOpt struct {
	dsn          string
	schemaName   string
	workloadPath string
}

func NewLoadWorkloadCmd() *cobra.Command {
	var opt loadWorkloadCmdOpt
	cmd := &cobra.Command{
		Use:   "load-workload",
		Short: "load tables and related statistics of the specified workload into your cluster",
		Long:  `load tables and related statistics of the specified workload into your cluster`,
		RunE: func(cmd *cobra.Command, args []string) error {
			// create a connection
			db, err := optimizer.NewTiDBWhatIfOptimizer(opt.dsn)
			utils.Must(err)
			loadWorkload(db, opt.workloadPath)
			return nil
		},
	}

	cmd.Flags().StringVar(&opt.dsn, "dsn", "root:@tcp(127.0.0.1:4000)/test", "dsn")
	cmd.Flags().StringVar(&opt.workloadPath, "workload-info-path", "", "workload info path")
	return cmd
}

func loadWorkload(db optimizer.WhatIfOptimizer, workloadPath string) {
	schemaSQLPath := path.Join(workloadPath, "schema.sql")
	schemaSQLs, err := utils.ParseRawSQLsFromFile(schemaSQLPath)
	utils.Must(err)
	for _, stmt := range schemaSQLs {
		utils.Must(db.Execute(stmt))
	}

	// load statistics
	statsFiles, err := os.ReadDir(path.Join(workloadPath, "stats"))
	utils.Must(err)
	for _, statsFile := range statsFiles {
		statsPath := path.Join(workloadPath, "stats", statsFile.Name())
		absStatsPath, err := filepath.Abs(statsPath)
		utils.Must(err, statsPath)
		mysql.RegisterLocalFile(absStatsPath)
		loadStatsSQL := fmt.Sprintf("load stats '%s'", absStatsPath)
		utils.Must(db.Execute(loadStatsSQL))
	}
}
