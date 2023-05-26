package main

import (
	"fmt"
	"os"
	"path"
	"strings"

	"github.com/pingcap/parser"
	"github.com/pingcap/parser/ast"
)

func must(err error, args ...interface{}) {
	if err != nil {
		fmt.Println("panic args: ", args)
		panic(err)
	}
}

// LoadWorkloadInfo loads workload info from the given path.
// TODO: for simplification, assume all SQLs are under the same schema here.
func LoadWorkloadInfo(schemaName, workloadInfoPath string) (WorkloadInfo, error) {
	sqlFilePath := path.Join(workloadInfoPath, "sqls.sql")
	rawSQLs, err := ParseRawSQLsFromFile(sqlFilePath)
	must(err, workloadInfoPath)
	var sqls []SQL
	for _, rawSQL := range rawSQLs {
		sqls = append(sqls, SQL{
			SchemaName: schemaName,
			Text:       rawSQL,
			Frequency:  1,
		})
	}

	schemaFilePath := path.Join(workloadInfoPath, "schema.sql")
	rawSQLs, err = ParseRawSQLsFromFile(schemaFilePath)
	if err != nil {
		return WorkloadInfo{}, err
	}
	var tableSchemas []TableSchema
	for _, rawSQL := range rawSQLs {
		tableSchema, err := ParseCreateTableStmt(schemaName, rawSQL)
		if err != nil {
			return WorkloadInfo{}, err
		}
		tableSchemas = append(tableSchemas, tableSchema)
	}

	// TODO: parse stats
	return WorkloadInfo{
		SQLs:         sqls,
		TableSchemas: tableSchemas,
	}, nil
}

func ParseRawSQLsFromFile(fpath string) ([]string, error) {
	data, err := os.ReadFile(fpath)
	if err != nil {
		return nil, err
	}
	lines := strings.Split(string(data), "\n")
	var filteredLines []string
	for _, line := range lines {
		line = strings.TrimSpace(line)
		if line == "" || strings.HasPrefix(line, "--") { // empty line or comment
			continue
		}
		filteredLines = append(filteredLines, line)
	}
	content := strings.Join(filteredLines, "\n")

	tmp := strings.Split(content, ";")
	var sqls []string
	for _, sql := range tmp {
		sql = strings.TrimSpace(sql)
		if sql == "" {
			continue
		}
		sqls = append(sqls, sql)
	}
	return sqls, nil
}

func ParseCreateTableStmt(schemaName, createTableStmt string) (TableSchema, error) {
	stmt, err := ParseOneSQL(createTableStmt)
	must(err, createTableStmt)
	createTable := stmt.(*ast.CreateTableStmt)
	t := TableSchema{
		SchemaName:     schemaName,
		TableName:      createTable.Table.Name.L,
		CreateStmtText: createTableStmt,
	}
	for _, colDef := range createTable.Cols {
		t.ColumnNames = append(t.ColumnNames, colDef.Name.Name.L)
	}
	// TODO: parse indexes
	return t, nil
}

func ParseOneSQL(sqlText string) (ast.StmtNode, error) {
	p := parser.New()
	return p.ParseOneStmt(sqlText, "", "")
}

func workloadQueryCost(info WorkloadInfo, optimizer WhatIfOptimizer) (float64, error) {
	var workloadCost float64
	for _, sql := range info.SQLs {
		if sql.Type() != SQLTypeSelect {
			continue
		}
		must(optimizer.Execute(`use ` + sql.SchemaName))
		cost, err := optimizer.GetPlanCost(sql.Text)
		must(err, sql.Text)
		workloadCost += cost * float64(sql.Frequency)
	}
	return workloadCost, nil
}
