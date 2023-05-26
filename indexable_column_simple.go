package main

import (
	"fmt"
	"strings"

	"github.com/pingcap/parser/ast"
	"github.com/pingcap/parser/opcode"
	_ "github.com/pingcap/tidb/types/parser_driver"
)

// simpleIndexableColumnsVisitor finds all columns that appear in any range-filter, order-by, or group-by clause.
type simpleIndexableColumnsVisitor struct {
	cols       map[string]struct{} // key = 'schema.table.column'
	schemaName string
	tables     []TableSchema
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

		key := fmt.Sprintf("%v.%v.%v", schemaName, tableName, colName)
		v.cols[key] = struct{}{}
	}
}

func (v *simpleIndexableColumnsVisitor) findTableName(schemaName, columnName string) string {
	// find the corresponding table
	for _, table := range v.tables {
		if table.SchemaName != schemaName {
			continue
		}
		for _, col := range table.ColumnNames {
			if col == columnName {
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
func FindIndexableColumnsSimple(workloadInfo WorkloadInfo) ([]IndexableColumn, error) {
	v := &simpleIndexableColumnsVisitor{
		cols:   make(map[string]struct{}),
		tables: workloadInfo.TableSchemas,
	}
	for _, sql := range workloadInfo.SQLs {
		stmt, err := ParseOneSQL(sql.Text)
		must(err, sql.Text)
		v.schemaName = sql.SchemaName
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
