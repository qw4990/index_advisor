package workload

import (
	"fmt"
	"github.com/qw4990/index_advisor/utils"
	"path"
	"strings"
	"unicode/utf8"

	"github.com/pingcap/parser/ast"
)

func FilterBySQLAlias(sqls utils.Set[SQL], alias []string) utils.Set[SQL] {
	aliasMap := make(map[string]struct{})
	for _, a := range alias {
		aliasMap[strings.TrimSpace(a)] = struct{}{}
	}

	filtered := utils.NewSet[SQL]()
	for _, sql := range sqls.ToList() {
		if _, ok := aliasMap[sql.Alias]; ok {
			filtered.Add(sql)
		}
	}
	return filtered
}

// createWorkloadFromRawStmt creates a WorkloadInfo from some raw SQLs.
// This function is mainly for testing.
func CreateWorkloadFromRawStmt(schemaName string, createTableStmts, rawSQLs []string) WorkloadInfo {
	sqls := utils.NewSet[SQL]()
	for _, rawSQL := range rawSQLs {
		sqls.Add(SQL{
			SchemaName: schemaName,
			Text:       rawSQL,
			Frequency:  1,
		})
	}
	tableSchemas := utils.NewSet[TableSchema]()
	for _, createStmt := range createTableStmts {
		tableSchema, err := ParseCreateTableStmt(schemaName, createStmt)
		utils.Must(err)
		tableSchemas.Add(tableSchema)
	}
	return WorkloadInfo{
		SQLs:         sqls,
		TableSchemas: tableSchemas,
	}
}

// LoadWorkloadInfo loads workload info from the given path.
func LoadWorkloadInfo(schemaName, workloadInfoPath string) (WorkloadInfo, error) {
	utils.Debugf("loading workload info from %s", workloadInfoPath)
	sqls := utils.NewSet[SQL]()
	if exist, isDir := utils.FileExists(path.Join(workloadInfoPath, "queries")); exist || isDir {
		rawSQLs, names, err := utils.ParseRawSQLsFromDir(path.Join(workloadInfoPath, "queries"))
		if err != nil {
			return WorkloadInfo{}, err
		}
		for i, rawSQL := range rawSQLs {
			sqls.Add(SQL{
				Alias:      strings.Split(names[i], ".")[0], // q1.sql, 2a.sql, etc.
				SchemaName: schemaName,                      // Notice: for simplification, assume all SQLs are under the same schema here.
				Text:       rawSQL,
				Frequency:  1,
			})
		}
	} else if exist, isDir := utils.FileExists(path.Join(workloadInfoPath, "queries.sql")); exist || !isDir {
		rawSQLs, err := utils.ParseRawSQLsFromFile(path.Join(workloadInfoPath, "queries.sql"))
		if err != nil {
			return WorkloadInfo{}, err
		}
		for i, rawSQL := range rawSQLs {
			sqls.Add(SQL{
				Alias:      fmt.Sprintf("q%v", i+1),
				SchemaName: schemaName, // Notice: for simplification, assume all SQLs are under the same schema here.
				Text:       rawSQL,
				Frequency:  1,
			})
		}
	} else {
		return WorkloadInfo{}, fmt.Errorf("can not find queries directory or queries.sql file under %s", workloadInfoPath)
	}

	schemaFilePath := path.Join(workloadInfoPath, "schema.sql")
	rawSQLs, err := utils.ParseRawSQLsFromFile(schemaFilePath)
	if err != nil {
		return WorkloadInfo{}, err
	}
	tableSchemas := utils.NewSet[TableSchema]()
	for _, rawSQL := range rawSQLs {
		tableSchema, err := ParseCreateTableStmt(schemaName, rawSQL)
		if err != nil {
			return WorkloadInfo{}, err
		}
		tableSchemas.Add(tableSchema)
	}

	// TODO: parse stats
	return WorkloadInfo{
		SQLs:         sqls,
		TableSchemas: tableSchemas,
	}, nil
}

// ParseCreateTableStmt parses a create table statement and returns a TableSchema.
func ParseCreateTableStmt(schemaName, createTableStmt string) (TableSchema, error) {
	stmt, err := utils.ParseOneSQL(createTableStmt)
	utils.Must(err, createTableStmt)
	createTable := stmt.(*ast.CreateTableStmt)
	t := TableSchema{
		SchemaName:     schemaName,
		TableName:      createTable.Table.Name.L,
		CreateStmtText: createTableStmt,
	}
	for _, colDef := range createTable.Cols {
		t.Columns = append(t.Columns, Column{
			SchemaName: schemaName,
			TableName:  createTable.Table.Name.L,
			ColumnName: colDef.Name.Name.L,
			ColumnType: colDef.Tp.Clone(),
		})
	}
	// TODO: parse indexes
	return t, nil
}

// TempIndexName returns a temp index name for the given columns.
func TempIndexName(cols ...Column) string {
	var names []string
	for _, col := range cols {
		names = append(names, col.ColumnName)
	}
	return fmt.Sprintf("idx_%v", strings.Join(names, "_"))
}

// FormatPlan formats the given plan.
func FormatPlan(p Plan) string {
	blank := strings.Repeat(" ", 4)
	nRows, nCols := len(p.Plan), len(p.Plan[0])
	lines := make([]string, nRows)
	for c := 0; c < nCols; c++ {
		maxLen := 0
		for r := 0; r < nRows; r++ {
			lines[r] += p.Plan[r][c] + blank
			maxLen = utils.Max(maxLen, utf8.RuneCountInString(lines[r]))
		}
		for r := 0; r < nRows; r++ {
			lines[r] += strings.Repeat(" ", maxLen-utf8.RuneCountInString(lines[r]))
		}
	}
	return strings.Join(lines, "\n")
}

func CheckWorkloadInfo(w WorkloadInfo) {
	for _, col := range w.IndexableColumns.ToList() {
		if col.SchemaName == "" || col.TableName == "" || col.ColumnName == "" {
			panic(fmt.Sprintf("invalid indexable column: %v", col))
		}
	}
	for _, sql := range w.SQLs.ToList() {
		if sql.SchemaName == "" || sql.Text == "" {
			panic(fmt.Sprintf("invalid sql: %v", sql))
		}
		for _, col := range sql.IndexableColumns.ToList() {
			if col.SchemaName == "" || col.TableName == "" || col.ColumnName == "" {
				panic(fmt.Sprintf("invalid indexable column: %v", col))
			}
		}
	}
	for _, tbl := range w.TableSchemas.ToList() {
		if tbl.SchemaName == "" || tbl.TableName == "" {
			panic(fmt.Sprintf("invalid table schema: %v", tbl))
		}
		for _, col := range tbl.Columns {
			if col.SchemaName == "" || col.TableName == "" || col.ColumnName == "" || col.ColumnType == nil {
				panic(fmt.Sprintf("invalid indexable column: %v", col))
			}
		}
		for _, idx := range tbl.Indexes {
			if idx.SchemaName == "" || idx.TableName == "" || idx.IndexName == "" {
				panic(fmt.Sprintf("invalid index: %v", idx))
			}
			for _, col := range idx.Columns {
				if col.SchemaName == "" || col.TableName == "" || col.ColumnName == "" {
					panic(fmt.Sprintf("invalid indexable column: %v", col))
				}
			}
		}
	}
}
