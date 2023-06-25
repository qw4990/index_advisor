package utils

import (
	"fmt"
	"github.com/pingcap/parser/ast"
	"path"
	"strings"
)

// FilterBySQLAlias filters Queries by their alias.
func FilterBySQLAlias(sqls Set[Query], alias []string) Set[Query] {
	aliasMap := make(map[string]struct{})
	for _, a := range alias {
		aliasMap[strings.TrimSpace(a)] = struct{}{}
	}

	filtered := NewSet[Query]()
	for _, sql := range sqls.ToList() {
		if _, ok := aliasMap[sql.Alias]; ok {
			filtered.Add(sql)
		}
	}
	return filtered
}

// CreateWorkloadFromRawStmt creates a WorkloadInfo from some raw Queries.
func CreateWorkloadFromRawStmt(schemaName string, createTableStmts, rawSQLs []string) (WorkloadInfo, error) {
	sqls := NewSet[Query]()
	for _, rawSQL := range rawSQLs {
		sqls.Add(Query{
			SchemaName: schemaName,
			Text:       rawSQL,
			Frequency:  1,
		})
	}
	tableSchemas := NewSet[TableSchema]()
	for _, createStmt := range createTableStmts {
		tableSchema, err := ParseCreateTableStmt(schemaName, createStmt)
		if err != nil {
			return WorkloadInfo{}, err
		}
		tableSchemas.Add(tableSchema)
	}
	return WorkloadInfo{
		Queries:      sqls,
		TableSchemas: tableSchemas,
	}, nil
}

// LoadWorkloadInfo loads workload info from the given path.
func LoadWorkloadInfo(schemaName, workloadInfoPath string) (WorkloadInfo, error) {
	Infof("loaf workload from %s", workloadInfoPath)
	sqls := NewSet[Query]()
	if exist, isDir := FileExists(path.Join(workloadInfoPath, "queries")); exist || isDir {
		queryDirPath := path.Join(workloadInfoPath, "queries")
		rawSQLs, names, err := ParseRawSQLsFromDir(queryDirPath)
		if err != nil {
			return WorkloadInfo{}, err
		}
		for i, rawSQL := range rawSQLs {
			sqls.Add(Query{
				Alias:      strings.Split(names[i], ".")[0], // q1.sql, 2a.sql, etc.
				SchemaName: schemaName,                      // Notice: for simplification, assume all Queries are under the same schema here.
				Text:       rawSQL,
				Frequency:  1,
			})
		}
		Infof("load %d queries from dir %s", len(rawSQLs), queryDirPath)
	} else if exist, isDir := FileExists(path.Join(workloadInfoPath, "queries.sql")); exist || !isDir {
		queryFilePath := path.Join(workloadInfoPath, "queries.sql")
		rawSQLs, err := ParseRawSQLsFromFile(queryFilePath)
		if err != nil {
			return WorkloadInfo{}, err
		}
		for i, rawSQL := range rawSQLs {
			sqls.Add(Query{
				Alias:      fmt.Sprintf("q%v", i+1),
				SchemaName: schemaName, // Notice: for simplification, assume all Queries are under the same schema here.
				Text:       rawSQL,
				Frequency:  1,
			})
		}
		Infof("load %d queries from %s", len(rawSQLs), queryFilePath)
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
		if GetStmtType(rawSQL) != StmtCreateTable {
			continue
		}

		tableSchema, err := ParseCreateTableStmt(schemaName, rawSQL)
		if err != nil {
			return WorkloadInfo{}, err
		}
		tableSchemas.Add(tableSchema)
	}

	// TODO: parse stats
	return WorkloadInfo{
		Queries:      sqls,
		TableSchemas: tableSchemas,
	}, nil
}

// ParseCreateTableStmt parses a create table statement and returns a TableSchema.
func ParseCreateTableStmt(schemaName, createTableStmt string) (TableSchema, error) {
	stmt, err := ParseOneSQL(createTableStmt)
	if err != nil {
		return TableSchema{}, err
	}
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
