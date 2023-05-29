package main

/*
	This algorithm resembles the index selection algorithm published in 1997 by Chaudhuri
	and Narasayya. Details can be found in the original paper:
	Surajit Chaudhuri, Vivek R. Narasayya: An Efficient Cost-Driven Index Selection
	Tool for Microsoft SQL Server. VLDB 1997: 146-155
	This implementation is the Golang version of github.com/hyrise/index_selection_evaluation/blob/refactoring/selection/algorithms/auto_admin_algorithm.py.
*/

// SelectIndexAAAlgo implements the auto-admin algorithm.
func SelectIndexAAAlgo(originalWorkloadInfo WorkloadInfo, compressedWorkloadInfo WorkloadInfo, parameter Parameter,
	columns []IndexableColumn, optimizer WhatIfOptimizer) (AdvisorResult, error) {
	aa := &autoAdmin{
		oriWorkloadInfo:  originalWorkloadInfo,
		compWorkloadInfo: compressedWorkloadInfo,
		indexableCols:    columns,
		optimizer:        optimizer,
		maxIndexes:       parameter.MaximumIndexesToRecommend,
		maxIndexesNative: 2,
		maxIndexWidth:    3,
	}
	aa.calculateBestIndexes()

	// TODO
	return AdvisorResult{}, nil
}

type autoAdmin struct {
	oriWorkloadInfo  WorkloadInfo
	compWorkloadInfo WorkloadInfo
	indexableCols    []IndexableColumn
	optimizer        WhatIfOptimizer

	maxIndexes       int // The algorithm stops as soon as it has selected #max_indexes indexes
	maxIndexesNative int // The number of indexes selected by a native enumeration.
	maxIndexWidth    int // The number of columns an index can contain at maximum.
}

func (aa *autoAdmin) calculateBestIndexes() []TableIndex {
	if aa.maxIndexes == 0 {
		return nil
	}

	var potentialIndexes []TableIndex
	for _, col := range aa.indexableCols {
		potentialIndexes = append(potentialIndexes, TableIndex{
			SchemaName:  col.SchemaName,
			TableName:   col.TableName,
			IndexName:   TempIndexName(col),
			ColumnNames: []string{col.ColumnName},
		})
	}

	for currentMaxIndexWidth := 1; currentMaxIndexWidth <= aa.maxIndexWidth; currentMaxIndexWidth++ {
		// TODO
	}

	// TODO
	return nil
}

func (aa *autoAdmin) selectIndexCandidates(potentialIndexes []TableIndex) []TableIndex {
	//candidates := make(map[string]TableIndex)
	//
	//for i, query := range aa.compWorkloadInfo.SQLs {
	//	if query.Type() != SQLTypeSelect {
	//		continue
	//	}
	//	queryWorkload := WorkloadInfo{
	//		SQLs:         aa.compWorkloadInfo.SQLs[i : i+1],
	//		TableSchemas: aa.compWorkloadInfo.TableSchemas,
	//		TableStats:   aa.compWorkloadInfo.TableStats,
	//		Plans:        aa.compWorkloadInfo.Plans[i : i+1],
	//		SampleRows:   aa.compWorkloadInfo.SampleRows,
	//	}
	//}

	// TODO
	return nil
}

func (aa *autoAdmin) potentialIndexesForQuery(queryWorkload WorkloadInfo, potentialIndexes []TableIndex) []TableIndex {
	// TODO
	return nil
}
