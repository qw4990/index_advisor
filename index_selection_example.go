package main

import (
	"fmt"
	"math/rand"
)

// SelectIndexExample select some indexes randomly.
func SelectIndexExample(originalWorkloadInfo WorkloadInfo, compressedWorkloadInfo WorkloadInfo, parameter Parameter,
	columns []Column, optimizer WhatIfOptimizer) (AdvisorResult, error) {
	originalCost, err := workloadQueryCost(originalWorkloadInfo, optimizer)
	if err != nil {
		return AdvisorResult{}, err
	}

	var indexes []Index
	for _, column := range columns {
		if rand.Intn(len(columns)) < parameter.MaximumIndexesToRecommend {
			idx := Index{
				SchemaName:  column.SchemaName,
				TableName:   column.TableName,
				IndexName:   fmt.Sprintf("key_%v", column.ColumnName),
				ColumnNames: []string{column.ColumnName},
			}
			indexes = append(indexes, idx)
			optimizer.CreateHypoIndex(idx.SchemaName, idx.TableName, idx.IndexName, idx.ColumnNames)
		}
	}

	optimizedCost, err := workloadQueryCost(originalWorkloadInfo, optimizer)
	return AdvisorResult{
		RecommendedIndexes:    indexes,
		OriginalWorkloadCost:  originalCost,
		OptimizedWorkloadCost: optimizedCost,
	}, err
}
