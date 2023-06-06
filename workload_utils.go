package main

import (
	"fmt"
	"path"
)

// createWorkloadFromRawStmt creates a WorkloadInfo from some raw SQLs.
// This function is mainly for testing.
func createWorkloadFromRawStmt(schemaName string, createTableStmts, rawSQLs []string) WorkloadInfo {
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
		must(err)
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
	sqlFilePath := path.Join(workloadInfoPath, "sqls.sql")
	rawSQLs, err := ParseRawSQLsFromFile(sqlFilePath)
	must(err, workloadInfoPath)
	sqls := NewSet[SQL]()
	for i, rawSQL := range rawSQLs {
		sqls.Add(SQL{
			Alias:      fmt.Sprintf("q%v", i+1),
			SchemaName: schemaName, // Notice: for simplification, assume all SQLs are under the same schema here.
			Text:       rawSQL,
			Frequency:  1,
		})
	}

	schemaFilePath := path.Join(workloadInfoPath, "schema.sql")
	rawSQLs, err = ParseRawSQLsFromFile(schemaFilePath)
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
