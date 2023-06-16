package advisor

import (
	"github.com/qw4990/index_advisor/optimizer"
	"github.com/qw4990/index_advisor/utils"
	wk "github.com/qw4990/index_advisor/workload"
)

/*
	This algorithm resembles the index selection algorithm published in 1997 by Chaudhuri
	and Narasayya. Details can be found in the original paper:
	Surajit Chaudhuri, Vivek R. Narasayya: An Efficient Cost-Driven Index Selection
	Tool for Microsoft SQL Server. VLDB 1997: 146-155
	This implementation is the Golang version of github.com/hyrise/index_selection_evaluation/blob/refactoring/selection/algorithms/auto_admin_algorithm.py.
*/

// SelectIndexAAAlgo implements the auto-admin algorithm.
func SelectIndexAAAlgo(workload wk.WorkloadInfo, parameter Parameter, optimizer optimizer.WhatIfOptimizer) (utils.Set[wk.Index], error) {
	aa := &autoAdmin{
		optimizer:  optimizer,
		maxIndexes: parameter.MaximumIndexesToRecommend,

		// TODO: make these 2 variables configurable.
		maxIndexesNative: 2,
		maxIndexWidth:    3,
	}
	utils.Debugf("starting auto-admin algorithm with max-indexes %d, max index-width %d, max index-naive %d", aa.maxIndexes, aa.maxIndexWidth, aa.maxIndexesNative)

	optimizer.ResetStats()
	bestIndexes := aa.calculateBestIndexes(workload)
	utils.Debugf("what-if optimizer stats: %v", optimizer.Stats().Format())
	return bestIndexes, nil
}

type autoAdmin struct {
	optimizer optimizer.WhatIfOptimizer

	maxIndexes       int // The algorithm stops as soon as it has selected #max_indexes indexes
	maxIndexesNative int // The number of indexes selected by a native enumeration.
	maxIndexWidth    int // The number of columns an index can contain at maximum.
}

func (aa *autoAdmin) calculateBestIndexes(workload wk.WorkloadInfo) utils.Set[wk.Index] {
	if aa.maxIndexes == 0 {
		return nil
	}

	potentialIndexes := utils.NewSet[wk.Index]() // each indexable column as a single-column index
	for _, col := range workload.IndexableColumns.ToList() {
		potentialIndexes.Add(wk.NewIndex(col.SchemaName, col.TableName, wk.TempIndexName(col), col.ColumnName))
	}

	currentBestIndexes := utils.NewSet[wk.Index]()
	for currentMaxIndexWidth := 1; currentMaxIndexWidth <= aa.maxIndexWidth; currentMaxIndexWidth++ {
		utils.Debugf("AutoAdmin Algo current max index width: %d", currentMaxIndexWidth)
		candidates := aa.selectIndexCandidates(workload, potentialIndexes)
		utils.Debugf("AutoAdmin Algo selectIndexCandidates: %v", candidates.Size())
		currentBestIndexes = aa.enumerateCombinations(workload, candidates)
		utils.Debugf("AutoAdmin Algo enumerateCombinations: %v", currentBestIndexes.Size())

		if currentMaxIndexWidth < aa.maxIndexWidth {
			// Update potential indexes for the next iteration
			potentialIndexes = currentBestIndexes
			potentialIndexes.AddSet(aa.createMultiColumnIndexes(workload, currentBestIndexes))
			potentialIndexes = aa.mergeCandidates(workload, potentialIndexes)
		}
	}

	limit := 0
	currentBestIndexes = aa.filterIndexes(currentBestIndexes)
	for currentBestIndexes.Size() < aa.maxIndexes {
		potentialIndexes = utils.DiffSet(potentialIndexes, currentBestIndexes)
		currentCost := EvaluateIndexConfCost(workload, aa.optimizer, currentBestIndexes)
		currentBestIndexes, _ = aa.enumerateGreedy(workload, currentBestIndexes, currentCost, potentialIndexes, aa.maxIndexes)
		currentBestIndexes = aa.filterIndexes(currentBestIndexes)
		limit++
		if limit > 5 {
			break
		}
	}

	return currentBestIndexes
}

func (aa *autoAdmin) createMultiColumnIndexes(workload wk.WorkloadInfo, indexes utils.Set[wk.Index]) utils.Set[wk.Index] {
	multiColumnCandidates := utils.NewSet[wk.Index]()
	for _, index := range indexes.ToList() {
		table, found := workload.TableSchemas.Find(wk.TableSchema{SchemaName: index.SchemaName, TableName: index.TableName})
		if !found {
			continue
		}
		tableColsSet := utils.ListToSet[wk.Column](table.Columns...)
		indexableColsSet := workload.IndexableColumns
		indexColsSet := utils.ListToSet[wk.Column](index.Columns...)
		for _, column := range utils.DiffSet(utils.AndSet(tableColsSet, indexableColsSet), indexColsSet).ToList() {
			cols := append([]wk.Column{}, index.Columns...)
			cols = append(cols, column)
			multiColumnCandidates.Add(wk.Index{
				SchemaName: index.SchemaName,
				TableName:  index.TableName,
				IndexName:  wk.TempIndexName(cols...),
				Columns:    cols,
			})
		}
	}
	return multiColumnCandidates
}

// filterIndexes filters some obviously unreasonable indexes.
// Rule 1: if index X is a prefix of index Y, then remove X.
// Rule 2(TBD): remove unnecessary suffix columns, e.g. X(a, b, c) to X(a, b) if no query can gain benefit from the suffix column c.
func (aa *autoAdmin) filterIndexes(indexes utils.Set[wk.Index]) utils.Set[wk.Index] {
	indexList := indexes.ToList()
	filteredIndexes := utils.NewSet[wk.Index]()
	for i, x := range indexList {
		filtered := false
		for j, y := range indexList {
			if i == j {
				continue
			}
			if y.PrefixContain(x) {
				filtered = true
				continue
			}
		}
		if !filtered {
			filteredIndexes.Add(x)
		}
	}
	return filteredIndexes
}

// mergeCandidates merges some index candidates.
// Rule 1: if candidate index X has no benefit, then remove X.
// Rule 2: if candidate index X is a prefix of some existing index in the workload, then remove X.
// Rule 3(TBD): if candidate index X is a prefix of another candidate Y and Y's workload cost is less than X's, then remove X.
func (aa *autoAdmin) mergeCandidates(workload wk.WorkloadInfo, candidates utils.Set[wk.Index]) utils.Set[wk.Index] {
	mergedCandidates := utils.NewSet[wk.Index]()
	candidatesList := candidates.ToList()
	var candidateCosts []wk.IndexConfCost
	for _, c := range candidatesList {
		candidateCosts = append(candidateCosts, EvaluateIndexConfCost(workload, aa.optimizer, utils.ListToSet(c)))
	}
	originalCost := EvaluateIndexConfCost(workload, aa.optimizer, utils.NewSet[wk.Index]())
	for i, x := range candidatesList {
		// rule 1
		if originalCost.Less(candidateCosts[i]) {
			continue
		}

		// rule 2
		table, ok := workload.TableSchemas.Find(wk.TableSchema{SchemaName: x.SchemaName, TableName: x.TableName})
		if !ok {
			panic("table not found")
		}
		for _, existingIndex := range table.Indexes {
			if existingIndex.PrefixContain(x) {
				continue
			}
		}

		//// rule 3
		//hitRule3 := false
		//for j, y := range candidatesList {
		//	if i == j {
		//		continue
		//	}
		//	// X is a prefix of Y and Y's cost is less than X's
		//	if y.PrefixContain(x) && candidateCosts[j].Less(candidateCosts[i]) {
		//		hitRule3 = true
		//		break
		//	}
		//}
		//if hitRule3 {
		//	continue
		//}
		mergedCandidates.Add(x)
	}
	return mergedCandidates
}

// selectIndexCandidates selects the best indexes for each single-query.
func (aa *autoAdmin) selectIndexCandidates(workload wk.WorkloadInfo, potentialIndexes utils.Set[wk.Index]) utils.Set[wk.Index] {
	candidates := utils.NewSet[wk.Index]()
	for _, query := range workload.SQLs.ToList() {
		if query.Type() != wk.SQLTypeSelect {
			continue
		}
		queryWorkload := wk.WorkloadInfo{ // each query as a workload
			SQLs:         utils.ListToSet(query),
			TableSchemas: workload.TableSchemas,
			TableStats:   workload.TableStats,
		}
		indexes := aa.potentialIndexesForQuery(query, potentialIndexes)
		candidates.AddSet(aa.enumerateCombinations(queryWorkload, indexes)) // best indexes for each single-query
	}
	return candidates
}

// potentialIndexesForQuery returns best recommended indexes of this workload from these candidates.
func (aa *autoAdmin) enumerateCombinations(workload wk.WorkloadInfo, candidateIndexes utils.Set[wk.Index]) utils.Set[wk.Index] {
	numberIndexesNaive := utils.Min(aa.maxIndexesNative, candidateIndexes.Size(), aa.maxIndexes)
	currentIndexes, cost := aa.enumerateNaive(workload, candidateIndexes, numberIndexesNaive)

	numberIndexes := utils.Min(aa.maxIndexes, candidateIndexes.Size())
	indexes, cost := aa.enumerateGreedy(workload, currentIndexes, cost, candidateIndexes, numberIndexes)
	return indexes
}

// enumerateGreedy finds the best combination of indexes with a greedy algorithm.
func (aa *autoAdmin) enumerateGreedy(workload wk.WorkloadInfo, currentIndexes utils.Set[wk.Index],
	currentCost wk.IndexConfCost, candidateIndexes utils.Set[wk.Index], numberIndexes int) (utils.Set[wk.Index], wk.IndexConfCost) {
	if currentIndexes.Size() >= numberIndexes {
		return currentIndexes, currentCost
	}

	var bestIndex wk.Index
	var bestCost wk.IndexConfCost
	for _, index := range candidateIndexes.ToList() {
		cost := EvaluateIndexConfCost(workload, aa.optimizer, utils.UnionSet(currentIndexes, utils.ListToSet(index)))
		if cost.Less(bestCost) {
			bestIndex, bestCost = index, cost
		}
	}
	if bestCost.Less(currentCost) {
		currentIndexes.Add(bestIndex)
		candidateIndexes.Remove(bestIndex)
		currentCost = bestCost
		return aa.enumerateGreedy(workload, currentIndexes, currentCost, candidateIndexes, numberIndexes)
	}

	return currentIndexes, currentCost
}

// enumerateNaive enumerates all possible combinations of indexes with at most numberIndexesNaive indexes and returns the best one.
func (aa *autoAdmin) enumerateNaive(workload wk.WorkloadInfo, candidateIndexes utils.Set[wk.Index], numberIndexesNaive int) (utils.Set[wk.Index], wk.IndexConfCost) {
	lowestCostIndexes := utils.NewSet[wk.Index]()
	var lowestCost wk.IndexConfCost
	for numberOfIndexes := 1; numberOfIndexes <= numberIndexesNaive; numberOfIndexes++ {
		for _, indexCombination := range utils.CombSet(candidateIndexes, numberOfIndexes) {
			cost := EvaluateIndexConfCost(workload, aa.optimizer, indexCombination)
			if cost.Less(lowestCost) {
				lowestCostIndexes = indexCombination
				lowestCost = cost
			}
		}
	}
	return lowestCostIndexes, lowestCost
}

func (aa *autoAdmin) potentialIndexesForQuery(query wk.SQL, potentialIndexes utils.Set[wk.Index]) utils.Set[wk.Index] {
	indexes := utils.NewSet[wk.Index]()
	for _, index := range potentialIndexes.ToList() {
		// The leading index column must be referenced by the query.
		if query.IndexableColumns.Contains(index.Columns[0]) {
			indexes.Add(index)
		}
	}
	return indexes
}
