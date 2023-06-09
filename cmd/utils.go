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
)

// loadWorkloadIntoCluster loads the schema the TiDB cluster
func loadSchemaIntoCluster(db optimizer.WhatIfOptimizer, schemaFilePath string) (dbName string, err error) {
	if schemaFilePath == "" {
		return "", nil
	}
	utils.Infof("load schema info from %v into the TiDB instance", schemaFilePath)
	rawSQLs, err := utils.ParseStmtsFromFile(schemaFilePath)
	if err != nil {
		return "", err
	}
	if len(rawSQLs) == 0 {
		return "", nil
	}

	currentDB := "test" // the default DB `test`
	for _, stmt := range rawSQLs {
		switch utils.GetStmtType(stmt) {
		case utils.StmtUseDB:
			currentDB = utils.GetDBNameFromUseDBStmt(stmt)
		case utils.StmtCreateDB:
			dbName := utils.GetDBNameFromCreateDBStmt(stmt)
			exist, err := dbExists(dbName, db)
			if err != nil {
				return "", err
			}
			if exist {
				continue
			}
		case utils.StmtCreateTable:
			table, err := utils.ParseCreateTableStmt(currentDB, stmt)
			if err != nil {
				return "", err
			}
			utils.Infof("create table %s.%s", table.SchemaName, table.TableName)
		}
		if err := db.Execute(stmt); err != nil {
			return "", err
		}
	}
	return currentDB, nil
}

// loadStatsIntoCluster loads the stats into the TiDB cluster
func loadStatsIntoCluster(db optimizer.WhatIfOptimizer, statsDirPath string) error {
	if statsDirPath == "" {
		return nil
	}
	utils.Infof("load stats info from %v into the TiDB instance", statsDirPath)
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
		statsPath := path.Join(statsDirPath, statsFile.Name())
		absStatsPath, err := filepath.Abs(statsPath)
		if err != nil {
			return err
		}
		tableName, err := getStatsFileTableName(absStatsPath)
		if err != nil {
			return err
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

// supportHypoIndex tests whether this TiDB version supports hypothetical indexes.
func supportHypoIndex(db optimizer.WhatIfOptimizer) bool {
	err := db.Execute(`drop hypo index hypo_index_test_name on test`)
	if strings.Contains(err.Error(), "You have an error in your SQL syntax") {
		return false
	}
	// if it's not a syntax error, we assume it supports Hypo Indexes.
	return true
}

func redactLogEnabled(db optimizer.WhatIfOptimizer) bool {
	rows, err := db.Query(`select @@global.tidb_redact_log`)
	if err != nil {
		return false
	}
	defer rows.Close()
	if !rows.Next() {
		return false
	}
	var redactLog string
	if err := rows.Scan(&redactLog); err != nil {
		return false
	}
	redactLog = strings.ToLower(redactLog)
	if redactLog == "1" || redactLog == "on" {
		return true
	}
	return false
}
