package main

type IndexableColumn struct {
	TableName  string
	ColumnName string
}

type IndexableColumnsFindingAlgo func(workloadInfo WorkloadInfo) []IndexableColumn

// FindIndexableColumnsSimple finds all columns that appear in any range-filter, order-by, or group-by clause.
func FindIndexableColumnsSimple(workloadInfo WorkloadInfo) []IndexableColumn {
	return nil
}
