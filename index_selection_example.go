package main

import (
	"fmt"
	"math/rand"
)

// SelectIndexExample select some indexes randomly.
func SelectIndexExample(workloadInfo WorkloadInfo, parameter Parameter,
	columns []IndexableColumn, optimizer WhatIfOptimizer) (AdvisorResult, error) {
	originalCost, err := workloadQueryCost(workloadInfo, optimizer)
	if err != nil {
		return AdvisorResult{}, err
	}

	var indexes []TableIndex
	for _, column := range columns {
		if rand.Intn(len(columns)) < parameter.MaximumIndexesToRecommend {
			idx := TableIndex{
				SchemaName:  column.SchemaName,
				TableName:   column.TableName,
				IndexName:   fmt.Sprintf("key_%v", column.ColumnName),
				ColumnNames: []string{column.ColumnName},
			}
			indexes = append(indexes, idx)
			optimizer.CreateHypoIndex(idx.SchemaName, idx.TableName, idx.IndexName, idx.ColumnNames)
		}
	}

	optimizedCost, err := workloadQueryCost(workloadInfo, optimizer)
	return AdvisorResult{
		RecommendedIndexes:    indexes,
		OriginalWorkloadCost:  originalCost,
		OptimizedWorkloadCost: optimizedCost,
	}, err
}
