package main

import (
	"fmt"
	"strings"

	"github.com/pingcap/tidb/parser"
	"github.com/pingcap/tidb/parser/ast"
	"github.com/pingcap/tidb/parser/opcode"
)

// simpleIndexableColumnsVisitor finds all columns that appear in any range-filter, order-by, or group-by clause.
type simpleIndexableColumnsVisitor struct {
	cols map[string]struct{} // key = 'schema.table.column'
}

func (v *simpleIndexableColumnsVisitor) Enter(n ast.Node) (node ast.Node, skipChildren bool) {
	switch x := n.(type) {
	case *ast.GroupByClause: // group by {col}
		for _, item := range x.Items {
			v.collectColumn(item)
		}
		return n, true
	case *ast.OrderByClause: // order by {col}
		for _, item := range x.Items {
			v.collectColumn(item)
		}
		return n, true
	case *ast.BetweenExpr: // {col} between ? and ?
		v.collectColumn(x.Expr)
	case *ast.PatternInExpr: // {col} in (?, ?, ...)
		v.collectColumn(x.Expr)
	case *ast.BinaryOperationExpr: // range predicates like `{col} > ?`
		switch x.Op {
		case opcode.EQ, opcode.LT, opcode.LE, opcode.GT, opcode.GE: // {col} = ?
			v.collectColumn(x.L)
			v.collectColumn(x.R)
		}
	default:
	}
	return n, false
}

func (v *simpleIndexableColumnsVisitor) collectColumn(n ast.Node) {
	switch x := n.(type) {
	case *ast.ColumnNameExpr:
		v.collectColumn(x.Name)
	case *ast.ColumnName:
		key := fmt.Sprintf("%v.%v.%v", x.Schema.L, x.Table.L, x.Name.L)
		v.cols[key] = struct{}{}
	}
}

func (v *simpleIndexableColumnsVisitor) Leave(n ast.Node) (node ast.Node, ok bool) {
	return n, true
}

// FindIndexableColumnsSimple finds all columns that appear in any range-filter, order-by, or group-by clause.
func FindIndexableColumnsSimple(workloadInfo WorkloadInfo) ([]IndexableColumn, error) {
	p := parser.New()
	v := &simpleIndexableColumnsVisitor{
		cols: make(map[string]struct{}),
	}

	for _, sql := range workloadInfo.SQLs {
		stmt, err := p.ParseOneStmt(sql.Text, "", "")
		if err != nil {
			return nil, err
		}
		stmt.Accept(v)
	}

	var cols []IndexableColumn
	for key := range v.cols {
		tmp := strings.Split(key, ".")
		cols = append(cols, IndexableColumn{
			SchemaName: tmp[0],
			TableName:  tmp[1],
			ColumnName: tmp[2],
		})
	}
	return cols, nil
}
