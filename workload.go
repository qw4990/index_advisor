package main

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
	SQLType    SQLType
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
