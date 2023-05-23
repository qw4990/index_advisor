package main

type WhatIfOptimizer interface {
	Execute(sql string) error

	CreateIndex(tableName string, indexName string, columnNames []string) error
	DropIndex(tableName string, indexName string) error

	GetPlan(query string) (planText string, err error)
	GetPlanCost(query string) (planCost float64, err error)
}

type TiDBWhatIfOptimizer struct {
}

func NewTiDBWhatIfOptimizer(DSN string) (WhatIfOptimizer, error) {
	return &TiDBWhatIfOptimizer{}, nil
}

func (o *TiDBWhatIfOptimizer) Execute(sql string) error {
	return nil
}

func (o *TiDBWhatIfOptimizer) CreateIndex(tableName string, indexName string, columnNames []string) error {
	return nil
}
func (o *TiDBWhatIfOptimizer) DropIndex(tableName string, indexName string) error {
	return nil
}

func (o *TiDBWhatIfOptimizer) GetPlan(query string) (planText string, err error) {
	return "", nil
}

func (o *TiDBWhatIfOptimizer) GetPlanCost(query string) (planCost float64, err error) {
	return 0, nil
}
