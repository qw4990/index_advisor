package main

import (
	"github.com/pingcap/tidb/parser"
	"github.com/pingcap/tidb/parser/ast"
)

type IndexableColumn struct {
	TableName   string
	ColumnNames []string
}

type IndexableColumnsFindingAlgo func(workloadInfo WorkloadInfo) ([]IndexableColumn, error)

// simpleIndexableColumnsVisitor finds all columns that appear in any range-filter, order-by, or group-by clause.
type simpleIndexableColumnsVisitor struct {
	cols map[string]map[string]struct{} // tableName -> columnName -> struct{}
}

func (v *simpleIndexableColumnsVisitor) Enter(n ast.Node) (node ast.Node, skipChildren bool) {
	// TODO
	return nil, false
}

func (v *simpleIndexableColumnsVisitor) Leave(n ast.Node) (node ast.Node, ok bool) {
	return n, true
}

// FindIndexableColumnsSimple finds all columns that appear in any range-filter, order-by, or group-by clause.
func FindIndexableColumnsSimple(workloadInfo WorkloadInfo) ([]IndexableColumn, error) {
	p := parser.New()
	v := &simpleIndexableColumnsVisitor{
		cols: make(map[string]map[string]struct{}),
	}

	for _, sql := range workloadInfo.SQLs {
		stmt, err := p.ParseOneStmt(sql.Text, "", "")
		if err != nil {
			return nil, err
		}
		stmt.Accept(v)
	}

	var cols []IndexableColumn
	for tbl, tblCols := range v.cols {
		var tmp IndexableColumn
		tmp.TableName = tbl
		for col := range tblCols {
			tmp.ColumnNames = append(tmp.ColumnNames, col)
		}
		cols = append(cols, tmp)
	}
	return cols, nil
}
