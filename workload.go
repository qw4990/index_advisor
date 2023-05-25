package main

import "strings"

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
	ColumnNames    []string
	Indexes        []TableIndex
	CreateStmtText string // `create table t (...)`
}

type TableStats struct {
	SchemaName    string
	TableName     string
	StatsFilePath string
}

type TableIndex struct {
	SchemaName  string
	TableName   string
	IndexName   string
	ColumnNames []string
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

func LoadWorkloadInfo(workloadInfoPath string) (WorkloadInfo, error) {
	return WorkloadInfo{}, nil
}
