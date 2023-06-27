package utils

import (
	"fmt"
	"github.com/pingcap/parser/ast"
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

// LoadQueries loads queries from the given path.
func LoadQueries(schemaName, queryPath string) (Set[Query], error) {
	queries := NewSet[Query]()
	if exist, isDir := FileExists(queryPath); exist || isDir {
		rawSQLs, names, err := ParseRawSQLsFromDir(queryPath)
		if err != nil {
			return nil, err
		}
		for i, rawSQL := range rawSQLs {
			queries.Add(Query{
				Alias:      strings.Split(names[i], ".")[0], // q1.sql, 2a.sql, etc.
				SchemaName: schemaName,                      // Notice: for simplification, assume all Queries are under the same schema here.
				Text:       rawSQL,
				Frequency:  1,
			})
		}
		Infof("load %d queries from dir %s", len(rawSQLs), queryPath)
	} else if exist, isDir := FileExists(queryPath); exist || !isDir {
		rawSQLs, err := ParseRawSQLsFromFile(queryPath)
		if err != nil {
			return nil, err
		}
		for i, rawSQL := range rawSQLs {
			queries.Add(Query{
				Alias:      fmt.Sprintf("q%v", i+1),
				SchemaName: schemaName, // Notice: for simplification, assume all Queries are under the same schema here.
				Text:       rawSQL,
				Frequency:  1,
			})
		}
		Infof("load %d queries from %s", len(rawSQLs), queryPath)
	} else {
		return nil, fmt.Errorf("can not find queries directory or queries.sql file under %s", queryPath)
	}
	return queries, nil
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
