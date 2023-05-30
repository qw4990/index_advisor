package main

import (
	"fmt"
	"strings"
)

type SQLType int

const (
	SQLTypeSelect SQLType = iota
	SQLTypeInsert
	SQLTypeUpdate
	SQLTypeOthers
)

type SQL struct { // DQL or DML
	SchemaName string
	Text       string
	Frequency  int
	Columns    []Column // columns in this SQL
}

func (sql SQL) Type() SQLType {
	text := strings.TrimSpace(sql.Text)
	if len(text) < 6 {
		return SQLTypeOthers
	}
	prefix := strings.ToLower(text[:6])
	if strings.HasPrefix(prefix, "select") {
		return SQLTypeSelect
	}
	if strings.HasPrefix(prefix, "insert") {
		return SQLTypeInsert
	}
	if strings.HasPrefix(prefix, "update") {
		return SQLTypeUpdate
	}
	return SQLTypeOthers
}

func (sql SQL) InColumns(col Column) bool {
	for _, c := range sql.Columns {
		if c.Key() == col.Key() {
			return true
		}
	}
	return false
}

type TableSchema struct {
	SchemaName     string
	TableName      string
	Columns        []Column
	Indexes        []Index
	CreateStmtText string // `create table t (...)`
}

type TableStats struct {
	SchemaName    string
	TableName     string
	StatsFilePath string
}

type Column struct {
	SchemaName string
	TableName  string
	ColumnName string
}

func (c Column) Key() string {
	return fmt.Sprintf("%v.%v.%v", c.SchemaName, c.TableName, c.ColumnName)
}

func (c Column) String() string {
	return fmt.Sprintf("%v.%v.%v", c.SchemaName, c.TableName, c.ColumnName)
}

type Index struct {
	SchemaName string
	TableName  string
	IndexName  string
	Columns    []Column
}

func (i Index) columnNames() []string {
	var names []string
	for _, col := range i.Columns {
		names = append(names, col.ColumnName)
	}
	return names
}

func (i Index) DDL() string {
	return fmt.Sprintf("CREATE INDEX %v ON %v.%v (%v)", i.IndexName, i.SchemaName, i.TableName, strings.Join(i.columnNames(), ", "))
}

func (i Index) Key() string {
	return fmt.Sprintf("%v.%v(%v)", i.SchemaName, i.TableName, strings.Join(i.columnNames(), ","))
}

type Plans struct {
}

type SampleRows struct {
	TableName string
}

type WorkloadInfo struct {
	SQLs         []SQL
	TableSchemas []TableSchema
	TableStats   []TableStats
	Plans        []Plans
	SampleRows   []SampleRows
}

func (w WorkloadInfo) FindTableSchema(schemaName, tableName string) (TableSchema, bool) {
	for _, t := range w.TableSchemas {
		if t.SchemaName == schemaName && t.TableName == tableName {
			return t, true
		}
	}
	return TableSchema{}, false
}
