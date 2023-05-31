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
func SelectIndexAAAlgo(originalWorkloadInfo, compressedWorkloadInfo WorkloadInfo, parameter Parameter, optimizer WhatIfOptimizer) (AdvisorResult, error) {
	aa := &autoAdmin{
		optimizer:  optimizer,
		maxIndexes: parameter.MaximumIndexesToRecommend,

		// TODO: make these 2 variables configurable.
		maxIndexesNative: 2,
		maxIndexWidth:    3,
	}
	Debugf("starting auto-admin algorithm with max-indexes %d, max index-width %d, max index-naive %d", aa.maxIndexes, aa.maxIndexWidth, aa.maxIndexesNative)

	bestIndexes := aa.calculateBestIndexes(compressedWorkloadInfo)

	var err error
	result := AdvisorResult{}
	result.RecommendedIndexes = bestIndexes.ToList()
	result.OriginalWorkloadCost, err = workloadQueryCost(originalWorkloadInfo, optimizer)
	must(err)
	result.OptimizedWorkloadCost = aa.simulateAndEvaluateCost(originalWorkloadInfo, bestIndexes)
	return result, nil
}

type autoAdmin struct {
	optimizer WhatIfOptimizer

	maxIndexes       int // The algorithm stops as soon as it has selected #max_indexes indexes
	maxIndexesNative int // The number of indexes selected by a native enumeration.
	maxIndexWidth    int // The number of columns an index can contain at maximum.
}

func (aa *autoAdmin) calculateBestIndexes(workload WorkloadInfo) Set[Index] {
	if aa.maxIndexes == 0 {
		return nil
	}

	potentialIndexes := NewSet[Index]() // each indexable column as a single-column index
	for _, col := range workload.IndexableColumns.ToList() {
		potentialIndexes.Add(NewIndex(col.SchemaName, col.TableName, TempIndexName(col), col.ColumnName))
	}

	currentBestIndexes := NewSet[Index]()
	for currentMaxIndexWidth := 1; currentMaxIndexWidth <= aa.maxIndexWidth; currentMaxIndexWidth++ {
		candidates := aa.selectIndexCandidates(workload, potentialIndexes)
		currentBestIndexes = aa.enumerateCombinations(workload, candidates)

		if currentMaxIndexWidth < aa.maxIndexWidth {
			// Update potential indexes for the next iteration
			potentialIndexes = currentBestIndexes
			potentialIndexes.AddSet(aa.createMultiColumnIndexes(workload, currentBestIndexes))
		}
	}

	return currentBestIndexes
}

func (aa *autoAdmin) createMultiColumnIndexes(workload WorkloadInfo, indexes Set[Index]) Set[Index] {
	multiColumnCandidates := NewSet[Index]()
	for _, index := range indexes.ToList() {
		table, found := workload.TableSchemas.Find(TableSchema{SchemaName: index.SchemaName, TableName: index.TableName})
		if !found {
			continue
		}
		tableColsSet := ListToSet[Column](table.Columns...)
		indexableColsSet := workload.IndexableColumns
		indexColsSet := ListToSet[Column](index.Columns...)
		for _, column := range DiffSet(AndSet(tableColsSet, indexableColsSet), indexColsSet).ToList() {
			cols := append([]Column{}, index.Columns...)
			cols = append(cols, column)
			multiColumnCandidates.Add(Index{
				SchemaName: index.SchemaName,
				TableName:  index.TableName,
				IndexName:  TempIndexName(cols...),
				Columns:    cols,
			})
		}
	}
	return multiColumnCandidates
}

// selectIndexCandidates selects the best indexes for each single-query.
func (aa *autoAdmin) selectIndexCandidates(workload WorkloadInfo, potentialIndexes Set[Index]) Set[Index] {
	candidates := NewSet[Index]()
	for _, query := range workload.SQLs.ToList() {
		if query.Type() != SQLTypeSelect {
			continue
		}
		queryWorkload := WorkloadInfo{ // each query as a workload
			SQLs:         ListToSet(query),
			TableSchemas: workload.TableSchemas,
			TableStats:   workload.TableStats,
		}
		indexes := aa.potentialIndexesForQuery(query, potentialIndexes)
		candidates.AddSet(aa.enumerateCombinations(queryWorkload, indexes)) // best indexes for each single-query
	}
	return candidates
}

// potentialIndexesForQuery returns best recommended indexes of this workload from these candidates.
func (aa *autoAdmin) enumerateCombinations(workload WorkloadInfo, candidateIndexes Set[Index]) Set[Index] {
	numberIndexesNaive := min(aa.maxIndexesNative, candidateIndexes.Size())
	currentIndexes, cost := aa.enumerateNaive(workload, candidateIndexes, numberIndexesNaive)

	numberIndexes := min(aa.maxIndexes, candidateIndexes.Size())
	indexes, cost := aa.enumerateGreedy(workload, currentIndexes, cost, candidateIndexes, numberIndexes)
	return indexes
}

func (aa *autoAdmin) enumerateGreedy(workload WorkloadInfo, currentIndexes Set[Index],
	currentCost float64, candidateIndexes Set[Index], numberIndexes int) (Set[Index], float64) {
	if currentIndexes.Size() > numberIndexes {
		return currentIndexes, currentCost
	}

	var bestIndex Index
	bestCost := math.MaxFloat64
	for _, index := range candidateIndexes.ToList() {
		cost := aa.simulateAndEvaluateCost(workload, UnionSet(currentIndexes, ListToSet(index)))
		if cost < bestCost {
			bestIndex, bestCost = index, cost
		}
	}
	if bestCost < currentCost {
		currentIndexes.Add(bestIndex)
		candidateIndexes.Remove(bestIndex)
		currentCost = bestCost
		return aa.enumerateGreedy(workload, currentIndexes, currentCost, candidateIndexes, numberIndexes)
	}

	return currentIndexes, currentCost
}

func (aa *autoAdmin) enumerateNaive(workload WorkloadInfo, candidateIndexes Set[Index], numberIndexesNaive int) (Set[Index], float64) {
	lowestCostIndexes := NewSet[Index]()
	lowestCost := math.MaxFloat64
	for numberOfIndexes := 1; numberOfIndexes <= numberIndexesNaive; numberOfIndexes++ {
		for _, indexCombination := range CombSet(candidateIndexes, numberOfIndexes) {
			cost := aa.simulateAndEvaluateCost(workload, indexCombination)
			if cost < lowestCost {
				lowestCostIndexes = indexCombination
				lowestCost = cost
			}
		}
	}
	return lowestCostIndexes, lowestCost
}

func (aa *autoAdmin) simulateAndEvaluateCost(workload WorkloadInfo, indexes Set[Index]) float64 {
	for _, index := range indexes.ToList() {
		must(aa.optimizer.CreateHypoIndex(index))
	}
	cost, err := workloadQueryCost(workload, aa.optimizer)
	must(err)
	for _, index := range indexes.ToList() {
		must(aa.optimizer.DropHypoIndex(index))
	}
	return cost
}

func (aa *autoAdmin) potentialIndexesForQuery(query SQL, potentialIndexes Set[Index]) Set[Index] {
	indexes := NewSet[Index]()
	for _, index := range potentialIndexes.ToList() {
		// The leading index column must be referenced by the query.
		if query.IndexableColumns.Contains(index.Columns[0]) {
			indexes.Add(index)
		}
	}
	return indexes
}
