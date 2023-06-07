package main

import (
	"github.com/pingcap/parser/ast"
	"github.com/pingcap/parser/mysql"
	"github.com/pingcap/parser/opcode"
	_ "github.com/pingcap/tidb/types/parser_driver"
)

// simpleIndexableColumnsVisitor finds all columns that appear in any range-filter, order-by, or group-by clause.
type simpleIndexableColumnsVisitor struct {
	schemaName  string
	tables      Set[TableSchema]
	cols        Set[Column] // key = 'schema.table.column'
	currentCols Set[Column] // columns related to the current sql
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
		c, ok := v.findColumnByName(schemaName, colName)
		if !ok || schemaName == "" || !v.checkColumnIndexableByType(c) {
			return // ignore this column
		}
		tableName = c.TableName
		col := NewColumn(schemaName, tableName, colName)
		v.cols.Add(col)
		v.currentCols.Add(col)
	}
}

func (v *simpleIndexableColumnsVisitor) checkColumnIndexableByType(c Column) bool {
	if c.ColumnType == nil {
		return false
	}
	switch c.ColumnType.Tp {
	case mysql.TypeTiny, mysql.TypeShort, mysql.TypeInt24, mysql.TypeLong, mysql.TypeLonglong, mysql.TypeYear,
		mysql.TypeFloat, mysql.TypeDouble, mysql.TypeNewDecimal,
		mysql.TypeDuration, mysql.TypeDate, mysql.TypeDatetime, mysql.TypeTimestamp:
		return true
	case mysql.TypeVarchar, mysql.TypeString, mysql.TypeVarString:
		return c.ColumnType.Flen <= 512
	}
	return false
}

func (v *simpleIndexableColumnsVisitor) findColumnByName(schemaName, columnName string) (Column, bool) {
	// find the corresponding table
	for _, table := range v.tables.ToList() {
		if table.SchemaName != schemaName {
			continue
		}
		for _, col := range table.Columns {
			if col.ColumnName == columnName {
				return col, true
			}
		}
	}
	return Column{}, false
}

func (v *simpleIndexableColumnsVisitor) Leave(n ast.Node) (node ast.Node, ok bool) {
	return n, true
}

// IndexableColumnsSelectionSimple finds all columns that appear in any range-filter, order-by, or group-by clause.
func IndexableColumnsSelectionSimple(workloadInfo *WorkloadInfo) error {
	v := &simpleIndexableColumnsVisitor{
		cols:   NewSet[Column](),
		tables: workloadInfo.TableSchemas,
	}
	sqls := workloadInfo.SQLs.ToList()
	for _, sql := range sqls {
		stmt, err := ParseOneSQL(sql.Text)
		must(err, sql.Text)
		v.schemaName = sql.SchemaName
		v.currentCols = NewSet[Column]()
		stmt.Accept(v)
		sql.IndexableColumns = v.currentCols
		workloadInfo.SQLs.Add(sql)
	}
	workloadInfo.IndexableColumns = v.cols
	return nil
}
