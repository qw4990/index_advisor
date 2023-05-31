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
	SchemaName       string
	Text             string
	Frequency        int
	IndexableColumns Set[Column] // Indexable columns related to this SQL
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

func NewColumn(schemaName, tableName, columnName string) Column {
	return Column{SchemaName: schemaName, TableName: tableName, ColumnName: columnName}
}

func NewColumns(schemaName, tableName string, columnNames ...string) []Column {
	var cols []Column
	for _, col := range columnNames {
		cols = append(cols, NewColumn(schemaName, tableName, col))
	}
	return cols
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

func NewIndex(schemaName, tableName, indexName string, columns ...string) Index {
	return Index{SchemaName: schemaName, TableName: tableName, IndexName: indexName, Columns: NewColumns(schemaName, tableName, columns...)}
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
	SQLs             []SQL
	TableSchemas     []TableSchema
	TableStats       []TableStats
	Plans            []Plans
	SampleRows       []SampleRows
	IndexableColumns Set[Column]
}

func (w WorkloadInfo) FindTableSchema(schemaName, tableName string) (TableSchema, bool) {
	for _, t := range w.TableSchemas {
		if t.SchemaName == schemaName && t.TableName == tableName {
			return t, true
		}
	}
	return TableSchema{}, false
}
