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
		panic(fmt.Sprintf("%v %v", err, args))
	}
}

// LoadWorkloadInfo loads workload info from the given path.
// TODO: for simplification, assume all SQLs are under the same schema here.
func LoadWorkloadInfo(schemaName, workloadInfoPath string) (WorkloadInfo, error) {
	sqlFilePath := path.Join(workloadInfoPath, "sqls.sql")
	data, err := os.ReadFile(sqlFilePath)
	if err != nil {
		return WorkloadInfo{}, err
	}
	var sqls []SQL
	for _, line := range strings.Split(string(data), ";") {
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}
		sqls = append(sqls, SQL{
			SchemaName: schemaName,
			Text:       line,
			Frequency:  1,
		})
	}

	schemaFilePath := path.Join(workloadInfoPath, "schema.sql")
	data, err = os.ReadFile(schemaFilePath)
	if err != nil {
		return WorkloadInfo{}, err
	}
	var tableSchemas []TableSchema
	for _, createStmt := range strings.Split(string(data), ";") {
		createStmt = strings.TrimSpace(createStmt)
		if createStmt == "" {
			continue
		}
		tableSchema, err := ParseCreateTableStmt(schemaName, createStmt)
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

func ParseCreateTableStmt(schemaName, createTableStmt string) (TableSchema, error) {
	stmt, err := ParseOneSQL(createTableStmt)
	if err != nil {
		return TableSchema{}, err
	}
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
