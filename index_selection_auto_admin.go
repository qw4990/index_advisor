package main

/*
	This algorithm resembles the index selection algorithm published in 1997 by Chaudhuri
	and Narasayya. Details can be found in the original paper:
	Surajit Chaudhuri, Vivek R. Narasayya: An Efficient Cost-Driven Index Selection
	Tool for Microsoft SQL Server. VLDB 1997: 146-155
	This implementation is the Golang version of github.com/hyrise/index_selection_evaluation/blob/refactoring/selection/algorithms/auto_admin_algorithm.py.
*/

// SelectIndexAAAlgo implements the auto-admin algorithm.
func SelectIndexAAAlgo(workload WorkloadInfo, parameter Parameter, optimizer WhatIfOptimizer) (Set[Index], error) {
	aa := &autoAdmin{
		optimizer:  optimizer,
		maxIndexes: parameter.MaximumIndexesToRecommend,

		// TODO: make these 2 variables configurable.
		maxIndexesNative: 2,
		maxIndexWidth:    3,
	}
	Debugf("starting auto-admin algorithm with max-indexes %d, max index-width %d, max index-naive %d", aa.maxIndexes, aa.maxIndexWidth, aa.maxIndexesNative)

	optimizer.ResetStats()
	bestIndexes := aa.calculateBestIndexes(workload)
	Debugf("what-if optimizer stats: %v", optimizer.Stats().Format())
	return bestIndexes, nil
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
		Debugf("AutoAdmin Algo current max index width: %d", currentMaxIndexWidth)
		candidates := aa.selectIndexCandidates(workload, potentialIndexes)
		Debugf("AutoAdmin Algo selectIndexCandidates: %v", candidates.Size())
		currentBestIndexes = aa.enumerateCombinations(workload, candidates)
		Debugf("AutoAdmin Algo enumerateCombinations: %v", currentBestIndexes.Size())

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
		potentialIndexes = DiffSet(potentialIndexes, currentBestIndexes)
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

// filterIndexes filters some obviously unreasonable indexes.
// Rule 1: if index X is a prefix of index Y, then remove X.
// Rule 2(TBD): remove unnecessary suffix columns, e.g. X(a, b, c) to X(a, b) if no query can gain benefit from the suffix column c.
func (aa *autoAdmin) filterIndexes(indexes Set[Index]) Set[Index] {
	indexList := indexes.ToList()
	filteredIndexes := NewSet[Index]()
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
func (aa *autoAdmin) mergeCandidates(workload WorkloadInfo, candidates Set[Index]) Set[Index] {
	mergedCandidates := NewSet[Index]()
	candidatesList := candidates.ToList()
	var candidateCosts []IndexConfCost
	for _, c := range candidatesList {
		candidateCosts = append(candidateCosts, EvaluateIndexConfCost(workload, aa.optimizer, ListToSet(c)))
	}
	originalCost := EvaluateIndexConfCost(workload, aa.optimizer, NewSet[Index]())
	for i, x := range candidatesList {
		// rule 1
		if originalCost.Less(candidateCosts[i]) {
			continue
		}

		// rule 2
		table, ok := workload.TableSchemas.Find(TableSchema{SchemaName: x.SchemaName, TableName: x.TableName})
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
	numberIndexesNaive := min(aa.maxIndexesNative, candidateIndexes.Size(), aa.maxIndexes)
	currentIndexes, cost := aa.enumerateNaive(workload, candidateIndexes, numberIndexesNaive)

	numberIndexes := min(aa.maxIndexes, candidateIndexes.Size())
	indexes, cost := aa.enumerateGreedy(workload, currentIndexes, cost, candidateIndexes, numberIndexes)
	return indexes
}

// enumerateGreedy finds the best combination of indexes with a greedy algorithm.
func (aa *autoAdmin) enumerateGreedy(workload WorkloadInfo, currentIndexes Set[Index],
	currentCost IndexConfCost, candidateIndexes Set[Index], numberIndexes int) (Set[Index], IndexConfCost) {
	if currentIndexes.Size() >= numberIndexes {
		return currentIndexes, currentCost
	}

	var bestIndex Index
	var bestCost IndexConfCost
	for _, index := range candidateIndexes.ToList() {
		cost := EvaluateIndexConfCost(workload, aa.optimizer, UnionSet(currentIndexes, ListToSet(index)))
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
func (aa *autoAdmin) enumerateNaive(workload WorkloadInfo, candidateIndexes Set[Index], numberIndexesNaive int) (Set[Index], IndexConfCost) {
	lowestCostIndexes := NewSet[Index]()
	var lowestCost IndexConfCost
	for numberOfIndexes := 1; numberOfIndexes <= numberIndexesNaive; numberOfIndexes++ {
		for _, indexCombination := range CombSet(candidateIndexes, numberOfIndexes) {
			cost := EvaluateIndexConfCost(workload, aa.optimizer, indexCombination)
			if cost.Less(lowestCost) {
				lowestCostIndexes = indexCombination
				lowestCost = cost
			}
		}
	}
	return lowestCostIndexes, lowestCost
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
