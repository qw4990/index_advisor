package main

import (
	"github.com/pingcap/parser/ast"
	"github.com/pingcap/parser/mysql"
	"github.com/pingcap/parser/opcode"
	_ "github.com/pingcap/tidb/types/parser_driver"
	"strings"
)

// simpleIndexableColumnsVisitor finds all columns that appear in any range-filter, order-by, or group-by clause.
type simpleIndexableColumnsVisitor struct {
	tables      Set[TableSchema]
	cols        Set[Column] // key = 'schema.table.column'
	currentSQL  SQL
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
			schemaName = v.currentSQL.SchemaName
		}
		colName = x.Name.L
		possibleColumns := v.matchPossibleColumns(schemaName, colName)
		if len(possibleColumns) == 0 || schemaName == "" {
			return // ignore this column
		}
		for _, c := range possibleColumns {
			if !v.checkColumnIndexableByType(c) {
				continue
			}
			tableName = c.TableName
			col := NewColumn(schemaName, tableName, colName)
			v.cols.Add(col)
			v.currentCols.Add(col)
		}
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

func (v *simpleIndexableColumnsVisitor) matchPossibleColumns(schemaName, columnName string) (cols []Column) {
	relatedTableNames := CollectTableNamesFromSQL(v.currentSQL.Text)
	for _, table := range v.tables.ToList() {
		if table.SchemaName != schemaName || !relatedTableNames.Contains(LowerString(table.TableName)) {
			continue
		}
		for _, col := range table.Columns {
			// it's hard to check which column belongs to which table, for simplification, just check whether this table name is in the query text.
			if !strings.Contains(strings.ToLower(v.currentSQL.Text), strings.ToLower(col.TableName)) {
				continue
			}
			if col.ColumnName == columnName {
				cols = append(cols, col)
			}
		}
	}
	return
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
		v.currentSQL = sql
		v.currentCols = NewSet[Column]()
		stmt.Accept(v)
		sql.IndexableColumns = v.currentCols
		workloadInfo.SQLs.Add(sql)
	}
	workloadInfo.IndexableColumns = v.cols
	return nil
}
