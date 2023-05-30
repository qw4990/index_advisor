package main

import (
	"github.com/pingcap/parser/ast"
	"github.com/pingcap/parser/opcode"
	_ "github.com/pingcap/tidb/types/parser_driver"
)

// simpleIndexableColumnsVisitor finds all columns that appear in any range-filter, order-by, or group-by clause.
type simpleIndexableColumnsVisitor struct {
	cols        map[Column]struct{} // key = 'schema.table.column'
	currentCols map[Column]struct{} // columns related to the current sql
	schemaName  string
	tables      []TableSchema
}

func (v *simpleIndexableColumnsVisitor) Enter(n ast.Node) (node ast.Node, skipChildren bool) {
	switch x := n.(type) {
	case *ast.GroupByClause: // group by {col}
		for _, item := range x.Items {
			v.collectColumn(item.Expr)
		}
		return n, true
	case *ast.OrderByClause: // order by {col}
		for _, item := range x.Items {
			v.collectColumn(item.Expr)
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
		var schemaName, tableName, colName string
		if x.Schema.L != "" {
			schemaName = x.Schema.L
		} else {
			schemaName = v.schemaName
		}
		colName = x.Name.L
		if x.Table.L != "" {
			tableName = x.Table.L
		} else {
			tableName = v.findTableName(schemaName, colName)
		}

		if schemaName == "" || tableName == "" {
			// TODO: can not find the corresponding schema and table name
		}
		col := Column{
			SchemaName: schemaName,
			TableName:  tableName,
			ColumnName: colName,
		}
		v.cols[col] = struct{}{}
		v.currentCols[col] = struct{}{}
	}
}

func (v *simpleIndexableColumnsVisitor) findTableName(schemaName, columnName string) string {
	// find the corresponding table
	for _, table := range v.tables {
		if table.SchemaName != schemaName {
			continue
		}
		for _, col := range table.Columns {
			if col.ColumnName == columnName {
				return table.TableName
			}
		}
	}
	return ""
}

func (v *simpleIndexableColumnsVisitor) Leave(n ast.Node) (node ast.Node, ok bool) {
	return n, true
}

// FindIndexableColumnsSimple finds all columns that appear in any range-filter, order-by, or group-by clause.
func FindIndexableColumnsSimple(workloadInfo WorkloadInfo) ([]Column, error) {
	v := &simpleIndexableColumnsVisitor{
		cols:   make(map[Column]struct{}),
		tables: workloadInfo.TableSchemas,
	}
	for i, sql := range workloadInfo.SQLs {
		stmt, err := ParseOneSQL(sql.Text)
		must(err, sql.Text)
		v.schemaName = sql.SchemaName
		v.currentCols = make(map[Column]struct{})
		stmt.Accept(v)

		workloadInfo.SQLs[i].Columns = nil
		for col := range v.currentCols {
			workloadInfo.SQLs[i].Columns = append(workloadInfo.SQLs[i].Columns, col)
		}
	}

	var cols []Column
	for col := range v.cols {
		cols = append(cols, col)
	}
	return cols, nil
}
