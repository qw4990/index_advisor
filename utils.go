package main

func must(err error) {
	if err != nil {
		panic(err)
	}
}

func workloadQueryCost(info WorkloadInfo, optimizer WhatIfOptimizer) (float64, error) {
	var workloadCost float64
	for _, sql := range info.SQLs {
		if sql.SQLType != SQLTypeSelect {
			continue
		}
		cost, err := optimizer.GetPlanCost(sql.Text)
		if err != nil {
			return 0, err
		}
		workloadCost += cost
	}
	return workloadCost, nil
}
