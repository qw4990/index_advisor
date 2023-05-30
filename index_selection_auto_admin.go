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

func (aa *autoAdmin) calculateBestIndexes() []Index {
	if aa.maxIndexes == 0 {
		return nil
	}

	var potentialIndexes, indexes []Index
	for _, col := range aa.indexableCols {
		potentialIndexes = append(potentialIndexes, Index{
			SchemaName: col.SchemaName,
			TableName:  col.TableName,
			IndexName:  TempIndexName(col),
			Columns:    []Column{col},
		})
	}

	for currentMaxIndexWidth := 1; currentMaxIndexWidth <= aa.maxIndexWidth; currentMaxIndexWidth++ {
		candidates := aa.selectIndexCandidates(potentialIndexes)
		indexes = aa.enumerateCombinations(aa.oriWorkloadInfo, candidates)

		if currentMaxIndexWidth < aa.maxIndexWidth {
			// Update potential indexes for the next iteration
			potentialIndexes = indexes
			potentialIndexes = append(potentialIndexes, aa.createMultiColumnIndexes()...)
		}
	}

	return indexes
}

func (aa *autoAdmin) createMultiColumnIndexes() []Index {
	// TODO
	return nil
}

func (aa *autoAdmin) selectIndexCandidates(potentialIndexes []Index) []Index {
	candidates := new(Set[Index])

	for i, query := range aa.compWorkloadInfo.SQLs {
		if query.Type() != SQLTypeSelect {
			continue
		}
		queryWorkload := WorkloadInfo{
			SQLs:         aa.compWorkloadInfo.SQLs[i : i+1],
			TableSchemas: aa.compWorkloadInfo.TableSchemas,
			TableStats:   aa.compWorkloadInfo.TableStats,
			Plans:        aa.compWorkloadInfo.Plans[i : i+1],
			SampleRows:   aa.compWorkloadInfo.SampleRows,
		}
		indexes := aa.potentialIndexesForQuery(query, potentialIndexes)
		indexes = aa.enumerateCombinations(queryWorkload, indexes)
		candidates.AddList(indexes...)
	}

	return candidates.ToList()
}

func (aa *autoAdmin) enumerateCombinations(workload WorkloadInfo, candidateIndexes []Index) []Index {
	numberIndexesNaive := int(math.Min(float64(aa.maxIndexesNative), float64(len(candidateIndexes))))
	currentIndexes, cost := aa.enumerateNaive(workload, candidateIndexes, numberIndexesNaive)

	numberIndexes := int(math.Min(float64(aa.maxIndexes), float64(len(candidateIndexes))))
	indexes, cost := aa.enumerateGreedy(workload, currentIndexes, cost, candidateIndexes, numberIndexes)
	return indexes
}

func (aa *autoAdmin) enumerateGreedy(workload WorkloadInfo, currentIndexes []Index, currentCost float64, candidateIndexes []Index, numberIndexes int) ([]Index, float64) {
	// TODO
	return nil, 0
}

func (aa *autoAdmin) enumerateNaive(workload WorkloadInfo, candidateIndexes []Index, numberIndexesNaive int) ([]Index, float64) {
	var lowestCostIndexes []Index
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

func (aa *autoAdmin) combinations(candidateIndexes []Index, numberIndexesNaive int) [][]Index {
	// TODO
	return nil
}

func (aa *autoAdmin) simulateAndEvaluateCost(indexes []Index) float64 {
	// TODO
	return 0
}

func (aa *autoAdmin) potentialIndexesForQuery(query SQL, potentialIndexes []Index) []Index {
	var indexes []Index
	for _, index := range potentialIndexes {
		// The leading index column must be referenced by the query.
		if query.InColumns(index.Columns[0]) {
			indexes = append(indexes, index)
		}
	}
	return indexes
}
