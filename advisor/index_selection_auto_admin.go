package advisor

import (
	"github.com/qw4990/index_advisor/optimizer"
	"github.com/qw4990/index_advisor/utils"
)

/*
	This algorithm resembles the index selection algorithm published in 1997 by Chaudhuri
	and Narasayya. Details can be found in the original paper:
	Surajit Chaudhuri, Vivek R. Narasayya: An Efficient Cost-Driven Index Selection
	Tool for Microsoft Query Server. VLDB 1997: 146-155
	This implementation is the Golang version of github.com/hyrise/index_selection_evaluation/blob/refactoring/selection/algorithms/auto_admin_algorithm.py.
*/

// SelectIndexAAAlgo implements the auto-admin algorithm.
func SelectIndexAAAlgo(workload utils.WorkloadInfo, parameter Parameter, optimizer optimizer.WhatIfOptimizer) (utils.Set[utils.Index], error) {
	aa := &autoAdmin{
		optimizer:     optimizer,
		maxIndexes:    parameter.MaxNumberIndexes,
		maxIndexWidth: parameter.MaxIndexWidth,
	}
	utils.Infof("starting auto-admin algorithm with max-indexes %d, max index-width %d", aa.maxIndexes, aa.maxIndexWidth)

	optimizer.ResetStats()
	bestIndexes, err := aa.calculateBestIndexes(workload)
	if err != nil {
		return nil, err
	}
	utils.Infof("what-if optimizer stats: %v", optimizer.Stats().Format())
	return bestIndexes, nil
}

type autoAdmin struct {
	optimizer optimizer.WhatIfOptimizer

	maxIndexes    int // The algorithm stops as soon as it has selected #max_indexes indexes
	maxIndexWidth int // The number of columns an index can contain at maximum.
}

func (aa *autoAdmin) calculateBestIndexes(workload utils.WorkloadInfo) (utils.Set[utils.Index], error) {
	if aa.maxIndexes == 0 {
		return nil, nil
	}

	potentialIndexes := utils.NewSet[utils.Index]() // each indexable column as a single-column index
	for _, col := range workload.IndexableColumns.ToList() {
		potentialIndexes.Add(utils.NewIndex(col.SchemaName, col.TableName, tempIndexName(col), col.ColumnName))
	}

	currentBestIndexes := utils.NewSet[utils.Index]()
	for currentMaxIndexWidth := 1; currentMaxIndexWidth <= aa.maxIndexWidth; currentMaxIndexWidth++ {
		utils.Infof("auto-admin algorithm: current index width is %d", currentMaxIndexWidth)
		candidates, err := aa.selectIndexCandidates(workload, potentialIndexes)
		if err != nil {
			return nil, err
		}

		//maxIndexes := aa.maxIndexes * (aa.maxIndexWidth - currentMaxIndexWidth + 1)
		maxIndexes := aa.maxIndexes
		utils.Infof("auto-admin algorithm: select best %v candidate indexes from %v candidates", maxIndexes, candidates.Size())
		currentBestIndexes, err = aa.enumerateCombinations(workload, candidates, maxIndexes)
		if err != nil {
			return nil, err
		}
		utils.Infof("auto-admin algorithm: select %v best candidate indexes", currentBestIndexes.Size())

		if currentMaxIndexWidth < aa.maxIndexWidth {
			// Update potential indexes for the next iteration
			potentialIndexes = currentBestIndexes
			potentialIndexes.AddSet(aa.createMultiColumnIndexes(workload, currentBestIndexes))
		}
	}

	currentBestIndexes, err := aa.heuristicMergeIndexes(currentBestIndexes, workload, aa.optimizer)
	if err != nil {
		return nil, err
	}

	utils.Infof("auto-admin algorithm: the number of candidate indexes before filter is %v", currentBestIndexes.Size())
	currentBestIndexes, err = aa.filterIndexes(workload, currentBestIndexes)
	if err != nil {
		return nil, err
	}

	// try to add more indexes if the number of indexes is less than maxIndexes
	for limit := 0; limit < 3 && currentBestIndexes.Size() < aa.maxIndexes; limit++ {
		potentialIndexes = utils.DiffSet(potentialIndexes, currentBestIndexes)
		currentCost, err := evaluateIndexConfCost(workload, aa.optimizer, currentBestIndexes)
		if err != nil {
			return nil, err
		}
		currentBestIndexes, _, err = aa.enumerateGreedy(workload, currentBestIndexes, currentCost, potentialIndexes, aa.maxIndexes)
		if err != nil {
			return nil, err
		}

		currentBestIndexes, err = aa.filterIndexes(workload, currentBestIndexes)
		if err != nil {
			return nil, err
		}
	}

	return currentBestIndexes, nil
}

func (aa *autoAdmin) heuristicCoveredIndexes(candidateIndexes utils.Set[utils.Index],
	w utils.WorkloadInfo, op optimizer.WhatIfOptimizer) utils.Set[utils.Index] {
	// TODO: build an index (b, a) for `select a from t where b=1` to convert IndexLookup to IndexScan
	return candidateIndexes
}

func (aa *autoAdmin) heuristicMergeIndexes(candidateIndexes utils.Set[utils.Index],
	w utils.WorkloadInfo, op optimizer.WhatIfOptimizer) (utils.Set[utils.Index], error) {
	// try to build index set {(c1), (c2)} for predicate like `where c1=1 or c2=2` so that index-merge can be applied.
	currentCost, err := evaluateIndexConfCost(w, op, candidateIndexes)
	if err != nil {
		return nil, err
	}

	for _, q := range w.Queries.ToList() {
		// get all DNF columns from the query
		dnfCols, err := utils.ParseDNFColumnsFromQuery(q)
		if err != nil {
			return nil, err
		}
		if dnfCols.Size() == 0 {
			continue
		}

		// create indexes for these DNF columns
		newIndexes := utils.NewSet[utils.Index]()
		for _, col := range dnfCols.ToList() {
			idx := utils.NewIndex(col.SchemaName, col.TableName, tempIndexName(col), col.ColumnName)
			contained := false
			for _, existingIndex := range candidateIndexes.ToList() {
				if existingIndex.PrefixContain(idx) {
					contained = true
					continue
				}
			}
			if !contained {
				newIndexes.Add(idx)
			}
		}
		if newIndexes.Size() == 0 {
			continue
		}

		// check whether these new indexes for IndexMerge can bring some benefits.
		newCandidateIndexes := utils.UnionSet(candidateIndexes, newIndexes)
		newCost, err := evaluateIndexConfCost(w, op, newCandidateIndexes)
		if err != nil {
			return nil, err
		}
		if newCost.Less(currentCost) {
			currentCost = newCost
			candidateIndexes, err = aa.filterIndexes(w, newCandidateIndexes)
			if err != nil {
				return nil, err
			}
		}
	}

	return candidateIndexes, nil
}

func (aa *autoAdmin) createMultiColumnIndexes(workload utils.WorkloadInfo, indexes utils.Set[utils.Index]) utils.Set[utils.Index] {
	multiColumnCandidates := utils.NewSet[utils.Index]()
	for _, index := range indexes.ToList() {
		table, found := workload.TableSchemas.Find(utils.TableSchema{SchemaName: index.SchemaName, TableName: index.TableName})
		if !found {
			continue
		}
		tableColsSet := utils.ListToSet[utils.Column](table.Columns...)
		indexableColsSet := workload.IndexableColumns
		indexColsSet := utils.ListToSet[utils.Column](index.Columns...)
		for _, column := range utils.DiffSet(utils.AndSet(tableColsSet, indexableColsSet), indexColsSet).ToList() {
			cols := append([]utils.Column{}, index.Columns...)
			cols = append(cols, column)
			multiColumnCandidates.Add(utils.Index{
				SchemaName: index.SchemaName,
				TableName:  index.TableName,
				IndexName:  tempIndexName(cols...),
				Columns:    cols,
			})
		}
	}
	return multiColumnCandidates
}

// filterIndexes filters some obviously unreasonable indexes.
// Rule 1: if index X is a prefix of index Y, then remove X.
// Rule 2: if index X has no any benefit, then remove X.
// Rule 3: if candidate index X is a prefix of some existing index in the workload, then remove X.
// Rule 4(TBD): remove unnecessary suffix columns, e.g. X(a, b, c) to X(a, b) if no query can gain benefit from the suffix column c.
func (aa *autoAdmin) filterIndexes(workload utils.WorkloadInfo, indexes utils.Set[utils.Index]) (utils.Set[utils.Index], error) {
	indexList := indexes.ToList()
	filteredIndexes := utils.NewSet[utils.Index]()
	originalCost, err := evaluateIndexConfCost(workload, aa.optimizer, indexes)
	if err != nil {
		return nil, err
	}
	for i, x := range indexList {
		filtered := false
		// rule 1
		for j, y := range indexList {
			if i == j {
				continue
			}
			if y.PrefixContain(x) {
				filtered = true
				continue
			}
		}
		if filtered {
			continue
		}

		// rule 2
		indexes.Remove(x)
		newCost, err := evaluateIndexConfCost(workload, aa.optimizer, indexes)
		if err != nil {
			return nil, err
		}
		indexes.Add(x)
		if !originalCost.Less(newCost) {
			continue
		}

		// rule 3
		table, ok := workload.TableSchemas.Find(utils.TableSchema{SchemaName: x.SchemaName, TableName: x.TableName})
		if ok {
			prefixContain := false
			for _, existingIndex := range table.Indexes {
				if existingIndex.PrefixContain(x) {
					prefixContain = true
				}
			}
			if prefixContain {
				continue
			}
		}

		filteredIndexes.Add(x)
	}
	return filteredIndexes, nil
}

// selectIndexCandidates selects the best indexes for each single-query.
func (aa *autoAdmin) selectIndexCandidates(workload utils.WorkloadInfo, potentialIndexes utils.Set[utils.Index]) (utils.Set[utils.Index], error) {
	utils.Debugf("auto-admin algorithm: select best index from %v candidates for each single query(total %v)", potentialIndexes.Size(), workload.Queries.Size())
	candidates := utils.NewSet[utils.Index]()
	for _, query := range workload.Queries.ToList() {
		queryWorkload := utils.WorkloadInfo{ // each query as a workload
			Queries:      utils.ListToSet(query),
			TableSchemas: workload.TableSchemas,
			TableStats:   workload.TableStats,
		}
		indexes := aa.potentialIndexesForQuery(query, potentialIndexes)

		bestPerQuery := 3 // keep 3 best indexes for each single-query
		bestQueryIndexes := utils.NewSet[utils.Index]()
		for i := 0; i < bestPerQuery; i++ {
			best, err := aa.enumerateCombinations(queryWorkload, indexes, 1)
			if err != nil {
				return nil, err
			}
			if best.Size() == 0 {
				break
			}
			bestQueryIndexes.AddSet(best)
			for _, index := range best.ToList() {
				indexes.Remove(index)
			}
			if bestQueryIndexes.Size() > bestPerQuery {
				break
			}
		}

		utils.Debugf("auto-admin algorithm: current best index for %s is %s", query.Alias, bestQueryIndexes.ToKeyList())
		candidates.AddSet(bestQueryIndexes)
	}
	return candidates, nil
}

// potentialIndexesForQuery returns best recommended indexes of this workload from these candidates.
func (aa *autoAdmin) enumerateCombinations(workload utils.WorkloadInfo,
	candidateIndexes utils.Set[utils.Index],
	maxNumberIndexes int) (utils.Set[utils.Index], error) {
	maxIndexesNative := 2
	if candidateIndexes.Size() > 50 {
		maxIndexesNative = 1
	}
	numberIndexesNaive := utils.Min(maxIndexesNative, candidateIndexes.Size(), maxNumberIndexes)
	currentIndexes, cost, err := aa.enumerateNaive(workload, candidateIndexes, numberIndexesNaive)
	if err != nil {
		return nil, err
	}

	numberIndexes := utils.Min(maxNumberIndexes, candidateIndexes.Size())
	indexes, cost, err := aa.enumerateGreedy(workload, currentIndexes, cost, candidateIndexes, numberIndexes)
	return indexes, err
}

// enumerateGreedy finds the best combination of indexes with a greedy algorithm.
func (aa *autoAdmin) enumerateGreedy(workload utils.WorkloadInfo, currentIndexes utils.Set[utils.Index],
	currentCost utils.IndexConfCost, candidateIndexes utils.Set[utils.Index], numberIndexes int) (utils.Set[utils.Index], utils.IndexConfCost, error) {
	if currentIndexes.Size() >= numberIndexes {
		return currentIndexes, currentCost, nil
	}

	var bestIndex utils.Index
	var bestCost utils.IndexConfCost
	for _, index := range candidateIndexes.ToList() {
		cost, err := evaluateIndexConfCost(workload, aa.optimizer, utils.UnionSet(currentIndexes, utils.ListToSet(index)))
		if err != nil {
			return nil, utils.IndexConfCost{}, err
		}
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

	return currentIndexes, currentCost, nil
}

// enumerateNaive enumerates all possible combinations of indexes with at most numberIndexesNaive indexes and returns the best one.
func (aa *autoAdmin) enumerateNaive(workload utils.WorkloadInfo, candidateIndexes utils.Set[utils.Index], numberIndexesNaive int) (utils.Set[utils.Index], utils.IndexConfCost, error) {
	lowestCostIndexes := utils.NewSet[utils.Index]()
	var lowestCost utils.IndexConfCost
	for numberOfIndexes := 1; numberOfIndexes <= numberIndexesNaive; numberOfIndexes++ {
		for _, indexCombination := range utils.CombSet(candidateIndexes, numberOfIndexes) {
			cost, err := evaluateIndexConfCost(workload, aa.optimizer, indexCombination)
			if err != nil {
				return nil, utils.IndexConfCost{}, err
			}
			if cost.Less(lowestCost) {
				lowestCostIndexes = indexCombination
				lowestCost = cost
			}
		}
	}
	return lowestCostIndexes, lowestCost, nil
}

func (aa *autoAdmin) potentialIndexesForQuery(query utils.Query, potentialIndexes utils.Set[utils.Index]) utils.Set[utils.Index] {
	indexes := utils.NewSet[utils.Index]()
	for _, index := range potentialIndexes.ToList() {
		// The leading index column must be referenced by the query.
		if query.IndexableColumns.Contains(index.Columns[0]) {
			indexes.Add(index)
		}
	}
	return indexes
}

//// mergeCandidates merges some index candidates.
//// Rule 1: if candidate index X has no benefit, then remove X.
//// Rule 2: if candidate index X is a prefix of some existing index in the workload, then remove X.
//// Rule 3(TBD): if candidate index X is a prefix of another candidate Y and Y's workload cost is less than X's, then remove X.
//func (aa *autoAdmin) mergeCandidates(workload utils.WorkloadInfo, candidates utils.Set[utils.Index]) (utils.Set[utils.Index], error) {
//	mergedCandidates := utils.NewSet[utils.Index]()
//	candidatesList := candidates.ToList()
//	var candidateCosts []utils.IndexConfCost
//	for _, c := range candidatesList {
//		cost, err := evaluateIndexConfCost(workload, aa.optimizer, utils.ListToSet(c))
//		if err != nil {
//			return nil, err
//		}
//		candidateCosts = append(candidateCosts, cost)
//	}
//	originalCost, err := evaluateIndexConfCost(workload, aa.optimizer, utils.NewSet[utils.Index]())
//	if err != nil {
//		return nil, err
//	}
//	for i, x := range candidatesList {
//		// rule 1
//		if originalCost.Less(candidateCosts[i]) {
//			continue
//		}
//
//		// rule 2
//		table, ok := workload.TableSchemas.Find(utils.TableSchema{SchemaName: x.SchemaName, TableName: x.TableName})
//		if !ok {
//			panic("table not found")
//		}
//		for _, existingIndex := range table.Indexes {
//			if existingIndex.PrefixContain(x) {
//				continue
//			}
//		}
//
//		//// rule 3
//		//hitRule3 := false
//		//for j, y := range candidatesList {
//		//	if i == j {
//		//		continue
//		//	}
//		//	// X is a prefix of Y and Y's cost is less than X's
//		//	if y.PrefixContain(x) && candidateCosts[j].Less(candidateCosts[i]) {
//		//		hitRule3 = true
//		//		break
//		//	}
//		//}
//		//if hitRule3 {
//		//	continue
//		//}
//		mergedCandidates.Add(x)
//	}
//	return mergedCandidates, nil
//}
