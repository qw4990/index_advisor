package main

import (
	"fmt"
	"math/rand"
)

// SelectIndexExample select some indexes randomly.
func SelectIndexExample(originalWorkloadInfo WorkloadInfo, compressedWorkloadInfo WorkloadInfo, parameter Parameter, optimizer WhatIfOptimizer) (Set[Index], error) {
	indexes := NewSet[Index]()
	for _, column := range compressedWorkloadInfo.IndexableColumns.ToList() {
		if rand.Intn(compressedWorkloadInfo.IndexableColumns.Size()) < parameter.MaximumIndexesToRecommend {
			idx := Index{
				SchemaName: column.SchemaName,
				TableName:  column.TableName,
				IndexName:  fmt.Sprintf("key_%v", column.ColumnName),
				Columns:    []Column{column},
			}
			indexes.Add(idx)
		}
	}
	return indexes, nil
}
