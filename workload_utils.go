package main

import (
	"fmt"
	"path"
	"strings"

	"github.com/pingcap/parser/ast"
)

// createWorkloadFromRawStmt creates a WorkloadInfo from some raw SQLs.
// This function is mainly for testing.
func createWorkloadFromRawStmt(schemaName string, createTableStmts, rawSQLs []string) WorkloadInfo {
	sqls := NewSet[SQL]()
	for _, rawSQL := range rawSQLs {
		sqls.Add(SQL{
			SchemaName: schemaName,
			Text:       rawSQL,
			Frequency:  1,
		})
	}
	tableSchemas := NewSet[TableSchema]()
	for _, createStmt := range createTableStmts {
		tableSchema, err := ParseCreateTableStmt(schemaName, createStmt)
		must(err)
		tableSchemas.Add(tableSchema)
	}
	return WorkloadInfo{
		SQLs:         sqls,
		TableSchemas: tableSchemas,
	}
}

// LoadWorkloadInfo loads workload info from the given path.
func LoadWorkloadInfo(schemaName, workloadInfoPath string) (WorkloadInfo, error) {
	Debugf("loading workload info from %s", workloadInfoPath)
	sqls := NewSet[SQL]()
	if exist, isDir := FileExists(path.Join(workloadInfoPath, "queries")); exist || isDir {
		rawSQLs, names, err := ParseRawSQLsFromDir(path.Join(workloadInfoPath, "queries"))
		if err != nil {
			return WorkloadInfo{}, err
		}
		for i, rawSQL := range rawSQLs {
			sqls.Add(SQL{
				Alias:      strings.Split(names[i], ".")[0], // q1.sql, 2a.sql, etc.
				SchemaName: schemaName,                      // Notice: for simplification, assume all SQLs are under the same schema here.
				Text:       rawSQL,
				Frequency:  1,
			})
		}
	} else if exist, isDir := FileExists(path.Join(workloadInfoPath, "queries.sql")); exist || !isDir {
		rawSQLs, err := ParseRawSQLsFromFile(path.Join(workloadInfoPath, "queries.sql"))
		if err != nil {
			return WorkloadInfo{}, err
		}
		for i, rawSQL := range rawSQLs {
			sqls.Add(SQL{
				Alias:      fmt.Sprintf("q%v", i+1),
				SchemaName: schemaName, // Notice: for simplification, assume all SQLs are under the same schema here.
				Text:       rawSQL,
				Frequency:  1,
			})
		}
	} else {
		return WorkloadInfo{}, fmt.Errorf("can not find queries directory or queries.sql file under %s", workloadInfoPath)
	}

	schemaFilePath := path.Join(workloadInfoPath, "schema.sql")
	rawSQLs, err := ParseRawSQLsFromFile(schemaFilePath)
	if err != nil {
		return WorkloadInfo{}, err
	}
	tableSchemas := NewSet[TableSchema]()
	for _, rawSQL := range rawSQLs {
		tableSchema, err := ParseCreateTableStmt(schemaName, rawSQL)
		if err != nil {
			return WorkloadInfo{}, err
		}
		tableSchemas.Add(tableSchema)
	}

	// TODO: parse stats
	return WorkloadInfo{
		SQLs:         sqls,
		TableSchemas: tableSchemas,
	}, nil
}

// ParseCreateTableStmt parses a create table statement and returns a TableSchema.
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
		t.Columns = append(t.Columns, Column{
			SchemaName: schemaName,
			TableName:  createTable.Table.Name.L,
			ColumnName: colDef.Name.Name.L,
		})
	}
	// TODO: parse indexes
	return t, nil
}

// EvaluateIndexConfCost evaluates the workload cost under the given indexes.
func EvaluateIndexConfCost(info WorkloadInfo, optimizer WhatIfOptimizer, indexes Set[Index]) IndexConfCost {
	for _, index := range indexes.ToList() {
		must(optimizer.CreateHypoIndex(index))
	}
	var workloadCost float64
	for _, sql := range info.SQLs.ToList() { // TODO: run them concurrently to save time
		if sql.Type() != SQLTypeSelect {
			continue
		}
		must(optimizer.Execute(`use ` + sql.SchemaName))
		p, err := optimizer.Explain(sql.Text)
		must(err, sql.Text)
		workloadCost += p.PlanCost() * float64(sql.Frequency)
	}
	for _, index := range indexes.ToList() {
		must(optimizer.DropHypoIndex(index))
	}
	var totCols int
	for _, index := range indexes.ToList() {
		totCols += len(index.Columns)
	}
	return IndexConfCost{workloadCost, totCols}
}

// TempIndexName returns a temp index name for the given columns.
func TempIndexName(cols ...Column) string {
	var names []string
	for _, col := range cols {
		names = append(names, col.ColumnName)
	}
	return fmt.Sprintf("idx_%v", strings.Join(names, "_"))
}

// FormatPlan formats the given plan.
func FormatPlan(p Plan) string {
	var lines []string
	for _, line := range p.Plan {
		lines = append(lines, strings.Join(line, "\t"))
	}
	return strings.Join(lines, "\n")
}

// checkWorkloadInfo checks whether this workload info is fulfilled.
func checkWorkloadInfo(w WorkloadInfo) {
	for _, col := range w.IndexableColumns.ToList() {
		if col.SchemaName == "" || col.TableName == "" || col.ColumnName == "" {
			panic(fmt.Sprintf("invalid indexable column: %v", col))
		}
	}
	for _, sql := range w.SQLs.ToList() {
		if sql.SchemaName == "" || sql.Text == "" {
			panic(fmt.Sprintf("invalid sql: %v", sql))
		}
		for _, col := range sql.IndexableColumns.ToList() {
			if col.SchemaName == "" || col.TableName == "" || col.ColumnName == "" {
				panic(fmt.Sprintf("invalid indexable column: %v", col))
			}
		}
	}
	for _, tbl := range w.TableSchemas.ToList() {
		if tbl.SchemaName == "" || tbl.TableName == "" {
			panic(fmt.Sprintf("invalid table schema: %v", tbl))
		}
		for _, col := range tbl.Columns {
			if col.SchemaName == "" || col.TableName == "" || col.ColumnName == "" {
				panic(fmt.Sprintf("invalid indexable column: %v", col))
			}
		}
		for _, idx := range tbl.Indexes {
			if idx.SchemaName == "" || idx.TableName == "" || idx.IndexName == "" {
				panic(fmt.Sprintf("invalid index: %v", idx))
			}
			for _, col := range idx.Columns {
				if col.SchemaName == "" || col.TableName == "" || col.ColumnName == "" {
					panic(fmt.Sprintf("invalid indexable column: %v", col))
				}
			}
		}
	}
}
