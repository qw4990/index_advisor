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
			return loadWorkload(db, opt.workloadPath)
		},
	}

	cmd.Flags().StringVar(&opt.dsn, "dsn", "root:@tcp(127.0.0.1:4000)", "dsn")
	cmd.Flags().StringVar(&opt.workloadPath, "workload-path", "", "workload dictionary path")
	return cmd
}

func loadWorkload(db optimizer.WhatIfOptimizer, workloadPath string) error {
	utils.Infof("load workload info from %s into the TiDB instance", workloadPath)
	schemaSQLPath := path.Join(workloadPath, "schema.sql")
	rawSQLs, err := utils.ParseRawSQLsFromFile(schemaSQLPath)
	if err != nil {
		return err
	}
	utils.Infof("load %d DDL statements from %s", len(rawSQLs), schemaSQLPath)
	if len(rawSQLs) == 0 {
		return nil
	}

	currentDB := ""
	existingTables := utils.NewSet[utils.TableName]()
	for _, stmt := range rawSQLs {
		switch utils.GetStmtType(stmt) {
		case utils.StmtUseDB:
			currentDB = utils.GetDBNameFromUseDBStmt(stmt)
		case utils.StmtCreateDB:
			dbName := utils.GetDBNameFromCreateDBStmt(stmt)
			exist, err := dbExists(dbName, db)
			if err != nil {
				return err
			}
			if exist {
				continue
			}
		case utils.StmtCreateTable:
			if currentDB == "" {
				return fmt.Errorf("no database specified before create table statement, please check %v to add `use {database}` before `create table ...`", schemaSQLPath)
			}
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
				existingTables.Add(utils.TableName{table.SchemaName, table.TableName})
				continue
			} else {
				utils.Infof("create table %s.%s", table.SchemaName, table.TableName)
			}
		}
		if err := db.Execute(stmt); err != nil {
			return err
		}
	}

	// load statistics
	statsDirPath := path.Join(workloadPath, "stats")
	exist, _ := utils.FileExists(statsDirPath)
	if !exist {
		utils.Infof("no stats directory %s, skip loading stats", statsDirPath)
		return nil
	}
	utils.Infof("load stats from %s", statsDirPath)
	statsFiles, err := os.ReadDir(statsDirPath)
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

func dbExists(schemaName string, db optimizer.WhatIfOptimizer) (bool, error) {
	q := fmt.Sprintf("select count(*) from INFORMATION_SCHEMA.SCHEMATA where lower(SCHEMA_NAME) = '%s'", strings.ToLower(schemaName))
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
