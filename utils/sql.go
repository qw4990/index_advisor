package utils

import (
	"strings"

	"github.com/pingcap/parser"
	"github.com/pingcap/parser/ast"
	_ "github.com/pingcap/tidb/types/parser_driver"
)

type StmtType int

const (
	StmtCreateDB StmtType = iota
	StmtUseDB
	StmtCreateTable
	StmtCreateIndex
	StmtUnknown
)

// GetStmtType returns the type of the given statement.
func GetStmtType(stmt string) StmtType {
	containAll := func(s string, substrs ...string) bool {
		s = strings.ToLower(s)
		for _, substr := range substrs {
			if !strings.Contains(s, substr) {
				return false
			}
		}
		return true
	}

	if containAll(stmt, "create", "database") {
		return StmtCreateDB
	} else if containAll(stmt, "create", "table") {
		return StmtCreateTable
	} else if containAll(stmt, "create", "index") {
		return StmtCreateIndex
	} else if containAll(stmt, "use") {
		return StmtUseDB
	}
	return StmtUnknown
}

// GetDBNameFromUseDBStmt returns the database name of the given `USE` statement.
func GetDBNameFromUseDBStmt(stmt string) string {
	db := strings.Split(stmt, " ")[1]
	db = strings.Trim(db, "` '\"")
	return db
}

// GetDBNameFromCreateDBStmt returns the database name of the given `CREATE DATABASE` statement.
func GetDBNameFromCreateDBStmt(stmt string) string {
	tmp := strings.Split(stmt, " ")
	db := tmp[len(tmp)-1]
	db = strings.Trim(db, "` '\"")
	return db
}

// ParseOneSQL parses the given Query text and returns the AST.
func ParseOneSQL(sqlText string) (ast.StmtNode, error) {
	p := parser.New()
	return p.ParseOneStmt(sqlText, "", "")
}

// NormalizeDigest normalizes the given Query text and returns the normalized Query text and its digest.
func NormalizeDigest(sqlText string) (string, string) {
	return parser.NormalizeDigest(sqlText)
}

type tableNameCollector struct {
	defaultSchemaName string
	tableNames        Set[TableName]
	cteNames          Set[TableName]
}

func (c *tableNameCollector) Enter(n ast.Node) (out ast.Node, skipChildren bool) {
	switch x := n.(type) {
	case *ast.WithClause:
		for _, cte := range x.CTEs {
			c.cteNames.Add(TableName{SchemaName: c.defaultSchemaName, TableName: cte.Name.String()})
		}
	case *ast.TableName:
		var t TableName
		if x.Schema.L == "" {
			t = TableName{SchemaName: c.defaultSchemaName, TableName: x.Name.String()}
		} else {
			t = TableName{SchemaName: x.Schema.O, TableName: x.Name.String()}
		}
		if !c.cteNames.Contains(t) {
			c.tableNames.Add(t)
		}
	}
	return n, false
}

func (c *tableNameCollector) Leave(n ast.Node) (out ast.Node, ok bool) {
	return n, true
}

// CollectTableNamesFromSQL returns all referenced table names in the given Query text.
// The returned format is `schemaName.tableName`.
// TODO: handle views and CTEs.
func CollectTableNamesFromSQL(defaultSchemaName, sqlText string) (Set[TableName], error) {
	node, err := ParseOneSQL(sqlText)
	if err != nil {
		return nil, err
	}
	c := &tableNameCollector{
		defaultSchemaName: defaultSchemaName,
		tableNames:        NewSet[TableName](),
		cteNames:          NewSet[TableName]()}
	node.Accept(c)
	return c.tableNames, nil
}

// CollectTableNamesFromQueries returns all referenced table names in the given queries.
func CollectTableNamesFromQueries(defaultSchemaName string, queries Set[Query]) (Set[TableName], error) {
	tableNames := NewSet[TableName]()
	for _, q := range queries.ToList() {
		names, err := CollectTableNamesFromSQL(defaultSchemaName, q.Text)
		if err != nil {
			return nil, err
		}
		tableNames.AddSet(names)
	}
	return tableNames, nil
}

// IsTiDBSystemTableName returns whether the given table name is a TiDB system table name.
func IsTiDBSystemTableName(t TableName) bool {
	schemaName := strings.ToLower(t.SchemaName)
	return schemaName == "information_schema" ||
		schemaName == "metrics_schema" ||
		schemaName == "performance_schema" ||
		schemaName == "mysql"
}
