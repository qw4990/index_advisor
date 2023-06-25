package advisor

import (
	"strings"

	"github.com/pingcap/parser/ast"
	"github.com/pingcap/parser/mysql"
	"github.com/pingcap/parser/opcode"
	_ "github.com/pingcap/tidb/types/parser_driver"
	"github.com/qw4990/index_advisor/utils"
)

// simpleIndexableColumnsVisitor finds all columns that appear in any range-filter, order-by, or group-by clause.
type simpleIndexableColumnsVisitor struct {
	tables      utils.Set[utils.TableSchema]
	cols        utils.Set[utils.Column] // key = 'schema.table.column'
	currentSQL  utils.SQL
	currentCols utils.Set[utils.Column] // columns related to the current utils.SQL
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
		possibleColumns, err := v.matchPossibleColumns(schemaName, colName)
		if err != nil {
			// TODO: log or return this error?
		}
		if len(possibleColumns) == 0 || schemaName == "" {
			return // ignore this column
		}
		for _, c := range possibleColumns {
			if !v.checkColumnIndexableByType(c) {
				continue
			}
			tableName = c.TableName
			col := utils.NewColumn(schemaName, tableName, colName)
			v.cols.Add(col)
			v.currentCols.Add(col)
		}
	}
}

func (v *simpleIndexableColumnsVisitor) checkColumnIndexableByType(c utils.Column) bool {
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

func (v *simpleIndexableColumnsVisitor) matchPossibleColumns(schemaName, columnName string) (cols []utils.Column, err error) {
	relatedTableNames, err := utils.CollectTableNamesFromSQL(schemaName, v.currentSQL.Text)
	if err != nil {
		return nil, err
	}
	for _, table := range v.tables.ToList() {
		if table.SchemaName != schemaName || !relatedTableNames.Contains(utils.TableName{schemaName, table.TableName}) {
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
func IndexableColumnsSelectionSimple(workloadInfo *utils.WorkloadInfo) error {
	v := &simpleIndexableColumnsVisitor{
		cols:   utils.NewSet[utils.Column](),
		tables: workloadInfo.TableSchemas,
	}
	sqls := workloadInfo.SQLs.ToList()
	for _, sql := range sqls {
		stmt, err := utils.ParseOneSQL(sql.Text)
		if err != nil {
			return err
		}
		v.currentSQL = sql
		v.currentCols = utils.NewSet[utils.Column]()
		stmt.Accept(v)
		sql.IndexableColumns = v.currentCols
		workloadInfo.SQLs.Add(sql)
	}
	workloadInfo.IndexableColumns = v.cols
	return nil
}
