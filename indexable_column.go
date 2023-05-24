package main

import "fmt"

type IndexableColumn struct {
	SchemaName string
	TableName  string
	ColumnName string
}

func (c IndexableColumn) String() string {
	return fmt.Sprintf("%v.%v.%v", c.SchemaName, c.TableName, c.ColumnName)
}

type IndexableColumnsFindingAlgo func(workloadInfo WorkloadInfo) ([]IndexableColumn, error)
