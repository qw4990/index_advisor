package main

type IndexableColumn struct {
	SchemaName string
	TableName  string
	ColumnName string
}

type IndexableColumnsFindingAlgo func(workloadInfo WorkloadInfo) ([]IndexableColumn, error)
