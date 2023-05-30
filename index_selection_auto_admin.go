package main

import "math"

/*
	This algorithm resembles the index selection algorithm published in 1997 by Chaudhuri
	and Narasayya. Details can be found in the original paper:
	Surajit Chaudhuri, Vivek R. Narasayya: An Efficient Cost-Driven Index Selection
	Tool for Microsoft SQL Server. VLDB 1997: 146-155
	This implementation is the Golang version of github.com/hyrise/index_selection_evaluation/blob/refactoring/selection/algorithms/auto_admin_algorithm.py.
*/

// SelectIndexAAAlgo implements the auto-admin algorithm.
func SelectIndexAAAlgo(originalWorkloadInfo WorkloadInfo, compressedWorkloadInfo WorkloadInfo, parameter Parameter,
	columns []Column, optimizer WhatIfOptimizer) (AdvisorResult, error) {
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
	indexableCols    []Column
	optimizer        WhatIfOptimizer

	maxIndexes       int // The algorithm stops as soon as it has selected #max_indexes indexes
	maxIndexesNative int // The number of indexes selected by a native enumeration.
	maxIndexWidth    int // The number of columns an index can contain at maximum.
}

func (aa *autoAdmin) calculateBestIndexes() Set[Index] {
	if aa.maxIndexes == 0 {
		return nil
	}

	potentialIndexes, indexes := NewSet[Index](), NewSet[Index]()
	for _, col := range aa.indexableCols { // each indexable column as a single-column index
		potentialIndexes.Add(Index{
			SchemaName: col.SchemaName,
			TableName:  col.TableName,
			IndexName:  TempIndexName(col),
			Columns:    []Column{col},
		})
	}

	for currentMaxIndexWidth := 1; currentMaxIndexWidth <= aa.maxIndexWidth; currentMaxIndexWidth++ {
		candidates := aa.selectIndexCandidates(aa.compWorkloadInfo, potentialIndexes)
		indexes = aa.enumerateCombinations(aa.compWorkloadInfo, candidates)

		if currentMaxIndexWidth < aa.maxIndexWidth {
			// Update potential indexes for the next iteration
			potentialIndexes = indexes
			potentialIndexes.AddSet(aa.createMultiColumnIndexes(aa.indexableCols, indexes))
		}
	}

	return indexes
}

func (aa *autoAdmin) createMultiColumnIndexes(indexableCols []Column, indexes Set[Index]) Set[Index] {
	//multiColumnCandidates := NewSet[Index]()
	//for _, index := range indexes.ToList() {
	//
	//}

	// TODO
	return nil
}

// selectIndexCandidates selects the best indexes for each single-query.
func (aa *autoAdmin) selectIndexCandidates(workload WorkloadInfo, potentialIndexes Set[Index]) Set[Index] {
	candidates := NewSet[Index]()
	for i, query := range workload.SQLs {
		if query.Type() != SQLTypeSelect {
			continue
		}
		queryWorkload := WorkloadInfo{ // each query as a workload
			SQLs:         workload.SQLs[i : i+1],
			TableSchemas: workload.TableSchemas,
			TableStats:   workload.TableStats,
			Plans:        workload.Plans[i : i+1],
			SampleRows:   workload.SampleRows,
		}
		indexes := aa.potentialIndexesForQuery(query, potentialIndexes)
		candidates.AddSet(aa.enumerateCombinations(queryWorkload, indexes)) // best indexes for each single-query
	}
	return candidates
}

func (aa *autoAdmin) enumerateCombinations(workload WorkloadInfo, candidateIndexes Set[Index]) Set[Index] {
	numberIndexesNaive := int(math.Min(float64(aa.maxIndexesNative), float64(candidateIndexes.Len())))
	currentIndexes, cost := aa.enumerateNaive(workload, candidateIndexes, numberIndexesNaive)

	numberIndexes := int(math.Min(float64(aa.maxIndexes), float64(candidateIndexes.Len())))
	indexes, cost := aa.enumerateGreedy(workload, currentIndexes, cost, candidateIndexes, numberIndexes)
	return indexes
}

func (aa *autoAdmin) enumerateGreedy(workload WorkloadInfo, currentIndexes Set[Index], currentCost float64, candidateIndexes Set[Index], numberIndexes int) (Set[Index], float64) {
	// TODO
	return nil, 0
}

func (aa *autoAdmin) enumerateNaive(workload WorkloadInfo, candidateIndexes Set[Index], numberIndexesNaive int) (Set[Index], float64) {
	lowestCostIndexes := NewSet[Index]()
	lowestCost := math.MaxFloat64
	for numberOfIndexes := 1; numberOfIndexes <= numberIndexesNaive; numberOfIndexes++ {
		for _, indexCombination := range aa.combinations(candidateIndexes, numberOfIndexes) {
			cost := aa.simulateAndEvaluateCost(indexCombination)
			if cost < lowestCost {
				lowestCostIndexes = indexCombination
				lowestCost = cost
			}
		}
	}
	return lowestCostIndexes, lowestCost
}

func (aa *autoAdmin) combinations(candidateIndexes Set[Index], numberIndexesNaive int) []Set[Index] {
	// TODO
	return nil
}

func (aa *autoAdmin) simulateAndEvaluateCost(indexes Set[Index]) float64 {
	// TODO
	return 0
}

func (aa *autoAdmin) potentialIndexesForQuery(query SQL, potentialIndexes Set[Index]) Set[Index] {
	indexes := NewSet[Index]()
	for _, index := range potentialIndexes.ToList() {
		// The leading index column must be referenced by the query.
		if query.InColumns(index.Columns[0]) {
			indexes.Add(index)
		}
	}
	return indexes
}
