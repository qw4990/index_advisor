package utils

import (
	"fmt"
	"github.com/pingcap/parser/ast"
	"path"
	"strings"
)

// FilterBySQLAlias filters SQLs by their alias.
func FilterBySQLAlias(sqls Set[SQL], alias []string) Set[SQL] {
	aliasMap := make(map[string]struct{})
	for _, a := range alias {
		aliasMap[strings.TrimSpace(a)] = struct{}{}
	}

	filtered := NewSet[SQL]()
	for _, sql := range sqls.ToList() {
		if _, ok := aliasMap[sql.Alias]; ok {
			filtered.Add(sql)
		}
	}
	return filtered
}

// CreateWorkloadFromRawStmt creates a WorkloadInfo from some raw SQLs.
func CreateWorkloadFromRawStmt(schemaName string, createTableStmts, rawSQLs []string) WorkloadInfo {
	sqls := NewSet[SQL]()
	for _, rawSQL := range rawSQLs {
		sqls.Add(SQL{
			SchemaName: schemaName,
			Text:       rawSQL,
			Frequency:  1,
		})
	}
	tableSchemas := NewSet[TableSchema]()
	for _, createStmt := range createTableStmts {
		tableSchema, err := ParseCreateTableStmt(schemaName, createStmt)
		Must(err)
		tableSchemas.Add(tableSchema)
	}
	return WorkloadInfo{
		SQLs:         sqls,
		TableSchemas: tableSchemas,
	}
}

// LoadWorkloadInfo loads workload info from the given path.
func LoadWorkloadInfo(schemaName, workloadInfoPath string) (WorkloadInfo, error) {
	Debugf("loading workload info from %s", workloadInfoPath)
	sqls := NewSet[SQL]()
	if exist, isDir := FileExists(path.Join(workloadInfoPath, "queries")); exist || isDir {
		rawSQLs, names, err := ParseRawSQLsFromDir(path.Join(workloadInfoPath, "queries"))
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
	} else if exist, isDir := FileExists(path.Join(workloadInfoPath, "queries.sql")); exist || !isDir {
		rawSQLs, err := ParseRawSQLsFromFile(path.Join(workloadInfoPath, "queries.sql"))
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
	rawSQLs, err := ParseRawSQLsFromFile(schemaFilePath)
	if err != nil {
		return WorkloadInfo{}, err
	}
	tableSchemas := NewSet[TableSchema]()
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
	stmt, err := ParseOneSQL(createTableStmt)
	Must(err, createTableStmt)
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