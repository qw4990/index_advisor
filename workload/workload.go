package workload

import (
	"fmt"
	"math"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/pingcap/parser/types"
	"github.com/qw4990/index_advisor/utils"
)

// SQLType represents the type of a SQL.
type SQLType int

const (
	SQLTypeSelect SQLType = iota
	SQLTypeInsert
	SQLTypeUpdate
	SQLTypeOthers
)

// SQL represents a SQL statement.
type SQL struct { // DQL or DML
	Alias            string
	SchemaName       string
	Text             string
	Frequency        int
	IndexableColumns utils.Set[Column] // Indexable columns related to this SQL
}

// Type returns the type of the SQL.
func (sql SQL) Type() SQLType {
	text := strings.TrimSpace(sql.Text)
	if len(text) < 6 {
		return SQLTypeOthers
	}
	prefix := strings.ToLower(text[:6])
	if strings.HasPrefix(prefix, "select") {
		return SQLTypeSelect
	}
	if strings.HasPrefix(prefix, "insert") {
		return SQLTypeInsert
	}
	if strings.HasPrefix(prefix, "update") {
		return SQLTypeUpdate
	}
	return SQLTypeOthers
}

// Key returns the key of the SQL.
func (sql SQL) Key() string {
	return sql.Text
}

// TableSchema represents the schema of a table.
type TableSchema struct {
	SchemaName     string
	TableName      string
	Columns        []Column
	Indexes        []Index
	CreateStmtText string // `create table t (...)`
}

// Key returns the key of the table schema.
func (t TableSchema) Key() string {
	return fmt.Sprintf("%v.%v", t.SchemaName, t.TableName)
}

// TableStats represents the statistics of a table.
type TableStats struct {
	SchemaName    string
	TableName     string
	StatsFilePath string
}

// Key returns the key of the table statistics.
func (t TableStats) Key() string {
	return fmt.Sprintf("%v.%v", t.SchemaName, t.TableName)
}

// Column represents a column.
type Column struct {
	SchemaName string
	TableName  string
	ColumnName string
	ColumnType *types.FieldType
}

// NewColumn creates a new column.
func NewColumn(schemaName, tableName, columnName string) Column {
	return Column{SchemaName: strings.ToLower(schemaName), TableName: strings.ToLower(tableName), ColumnName: strings.ToLower(columnName)}
}

// NewColumns creates new columns.
func NewColumns(schemaName, tableName string, columnNames ...string) []Column {
	var cols []Column
	for _, col := range columnNames {
		cols = append(cols, NewColumn(schemaName, tableName, col))
	}
	return cols
}

// Key returns the key of the column.
func (c Column) Key() string {
	return fmt.Sprintf("%v.%v.%v", c.SchemaName, c.TableName, c.ColumnName)
}

// String returns the string representation of the column.
func (c Column) String() string {
	return fmt.Sprintf("%v.%v.%v", c.SchemaName, c.TableName, c.ColumnName)
}

// Index represents an index.
type Index struct {
	SchemaName string
	TableName  string
	IndexName  string
	Columns    []Column
}

// NewIndex creates a new index.
func NewIndex(schemaName, tableName, indexName string, columns ...string) Index {
	return Index{SchemaName: strings.ToLower(schemaName), TableName: strings.ToLower(tableName), IndexName: strings.ToLower(indexName), Columns: NewColumns(schemaName, tableName, columns...)}
}

// ColumnNames returns the column names of the index.
func (i Index) ColumnNames() []string {
	var names []string
	for _, col := range i.Columns {
		names = append(names, col.ColumnName)
	}
	return names
}

// DDL returns the DDL of the index.
func (i Index) DDL() string {
	return fmt.Sprintf("CREATE INDEX %v ON %v.%v (%v)", i.IndexName, i.SchemaName, i.TableName, strings.Join(i.ColumnNames(), ", "))
}

// Key returns the key of the index.
func (i Index) Key() string {
	return fmt.Sprintf("%v.%v(%v)", i.SchemaName, i.TableName, strings.Join(i.ColumnNames(), ","))
}

// PrefixContain returns whether j is a prefix of i.
func (i Index) PrefixContain(j Index) bool {
	if i.SchemaName != j.SchemaName || i.TableName != j.TableName || len(i.Columns) < len(j.Columns) {
		return false
	}
	for k := range j.Columns {
		if i.Columns[k].ColumnName != j.Columns[k].ColumnName {
			return false
		}
	}
	return true
}

// Plan represents a plan.
type Plan [][]string

// IsExecuted returns whether this plan is executed.
func (p Plan) IsExecuted() bool {
	// | id | estRows  | estCost | actRows | task | access object | execution info | operator info | memory | disk |
	return len(p[0]) == 10
}

// PlanCost returns the cost of the plan.
func (p Plan) PlanCost() float64 {
	v, err := strconv.ParseFloat(p[0][2], 64)
	utils.Must(err)
	return v
}

// ExecTime returns the execution time of the plan.
func (p Plan) ExecTime() time.Duration {
	if !p.IsExecuted() {
		return 0
	}

	//| TableReader_5 | 10000.00 | 177906.67 | 0 | root | - | time:3.15ms, loops:1, ... | data:TableFullScan_4 | 174 Bytes | N/A |
	execInfo := p[0][6]
	b := strings.Index(execInfo, "time:")
	e := strings.Index(execInfo, ",")
	tStr := execInfo[b+len("time:") : e]
	d, err := time.ParseDuration(tStr)
	utils.Must(err)
	return d
}

func (p Plan) Format() string {
	blank := strings.Repeat(" ", 4)
	nRows, nCols := len(p), len(p[0])
	lines := make([]string, nRows)
	for c := 0; c < nCols; c++ {
		maxLen := 0
		for r := 0; r < nRows; r++ {
			lines[r] += p[r][c] + blank
			maxLen = utils.Max(maxLen, utf8.RuneCountInString(lines[r]))
		}
		for r := 0; r < nRows; r++ {
			lines[r] += strings.Repeat(" ", maxLen-utf8.RuneCountInString(lines[r]))
		}
	}
	return strings.Join(lines, "\n")
}

// WorkloadInfo represents the workload information.
type WorkloadInfo struct {
	SQLs             utils.Set[SQL]
	TableSchemas     utils.Set[TableSchema]
	TableStats       utils.Set[TableStats]
	IndexableColumns utils.Set[Column]
}

// IndexConfCost is the cost of a index configuration.
type IndexConfCost struct {
	TotalWorkloadQueryCost    float64
	TotalNumberOfIndexColumns int
}

// Less returns whether the cost of c is less than the cost of other.
func (c IndexConfCost) Less(other IndexConfCost) bool {
	if c.TotalNumberOfIndexColumns == 0 { // not initialized
		return false
	}
	if other.TotalNumberOfIndexColumns == 0 { // not initialized
		return true
	}
	cc, cOther := c.TotalWorkloadQueryCost, other.TotalWorkloadQueryCost
	if math.Abs(cc-cOther) < 10 || math.Abs(cc-cOther)/math.Max(cc, cOther) < 0.01 {
		// if they have the same cost, then the less columns, the better.
		return c.TotalNumberOfIndexColumns < other.TotalNumberOfIndexColumns
	}
	return c.TotalWorkloadQueryCost < other.TotalWorkloadQueryCost
}