package cmd

import (
	"encoding/json"
	"fmt"
	"os"
	"path"
	"path/filepath"
	"strings"

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
			if err != nil {
				return err
			}
			return loadWorkload(db, "test", opt.workloadPath)
		},
	}

	cmd.Flags().StringVar(&opt.dsn, "dsn", "root:@tcp(127.0.0.1:4000)/test", "dsn")
	cmd.Flags().StringVar(&opt.workloadPath, "workload-info-path", "", "workload info path")
	return cmd
}

func loadWorkload(db optimizer.WhatIfOptimizer, defaultDB, workloadPath string) error {
	schemaSQLPath := path.Join(workloadPath, "schema.sql")
	rawSQLs, err := utils.ParseRawSQLsFromFile(schemaSQLPath)
	if err != nil {
		return err
	}

	currentDB := defaultDB
	existingTables := utils.NewSet[utils.TableName]()
	for _, stmt := range rawSQLs {
		switch utils.GetStmtType(stmt) {
		case utils.StmtUseDB:
			currentDB = utils.GetDBNameFromUseDBStmt(stmt)
		case utils.StmtCreateTable:
			table, err := utils.ParseCreateTableStmt(currentDB, stmt)
			if err != nil {
				return err
			}
			exist, err := tableExists(table.SchemaName, table.TableName, db)
			if err != nil {
				return err
			}
			if exist {
				utils.Infof("table %s.%s already exists, skip creating it", table.SchemaName, table.TableName)
				continue
			} else {
				utils.Infof("create table %s.%s", table.SchemaName, table.TableName)
				existingTables.Add(utils.TableName{table.SchemaName, table.TableName})
			}
		}
		if err := db.Execute(stmt); err != nil {
			return err
		}
	}

	// load statistics
	statsFiles, err := os.ReadDir(path.Join(workloadPath, "stats"))
	if err != nil {
		return err
	}
	for _, statsFile := range statsFiles {
		statsPath := path.Join(workloadPath, "stats", statsFile.Name())
		absStatsPath, err := filepath.Abs(statsPath)
		if err != nil {
			return err
		}
		tableName, err := getStatsFileTableName(absStatsPath)
		if err != nil {
			return err
		}
		if existingTables.Contains(tableName) {
			continue
		}

		utils.Infof("load stats for table %s from %s", tableName, statsPath)
		mysql.RegisterLocalFile(absStatsPath)
		loadStatsSQL := fmt.Sprintf("load stats '%s'", absStatsPath)
		if err := db.Execute(loadStatsSQL); err != nil {
			return err
		}
	}
	return nil
}

type statsJSON struct {
	DatabaseName string `json:"database_name"`
	TableName    string `json:"table_name"`
}

func getStatsFileTableName(statsFile string) (utils.TableName, error) {
	var stats statsJSON
	data, err := os.ReadFile(statsFile)
	if err != nil {
		return utils.TableName{}, err
	}
	if err := json.Unmarshal(data, &stats); err != nil {
		return utils.TableName{}, err
	}
	return utils.TableName{stats.DatabaseName, stats.TableName}, nil
}

func tableExists(schemaName, tableName string, db optimizer.WhatIfOptimizer) (bool, error) {
	q := fmt.Sprintf("select count(*) from information_schema.TABLES where lower(table_schema) = '%s' and lower(table_name)='%s'",
		strings.ToLower(schemaName), strings.ToLower(tableName))
	r, err := db.Query(q)
	if err != nil {
		return false, err
	}
	r.Next()
	var count int
	if err := r.Scan(&count); err != nil {
		return false, err
	}
	if err := r.Close(); err != nil {
		return false, err
	}
	return count > 0, nil
}
