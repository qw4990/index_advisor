package main

type SQL struct { // DQL or DML
	SchemaName string
	Text       string
	Frequency  int
}

type TableSchema struct {
	SchemaName     string
	TableName      string
	ColumnNames    []string
	CreateStmtText string // `create table t (...)`
}

type TableStats struct {
	SchemaName    string
	TableName     string
	StatsFilePath string
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
