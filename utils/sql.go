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
	} else if containAll(stmt, "use") {
		return StmtUseDB
	} else if containAll(stmt, "create", "table") {
		return StmtCreateTable
	}
	return StmtUnknown
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
}

func (c *tableNameCollector) Enter(n ast.Node) (out ast.Node, skipChildren bool) {
	switch x := n.(type) {
	case *ast.TableName:
		if x.Schema.L == "" {
			c.tableNames.Add(TableName{SchemaName: c.defaultSchemaName, TableName: x.Name.String()})
		} else {
			c.tableNames.Add(TableName{SchemaName: x.Schema.O, TableName: x.Name.String()})
		}
	}
	return n, false
}

func (c *tableNameCollector) Leave(n ast.Node) (out ast.Node, ok bool) {
	return n, true
}

// CollectTableNamesFromSQL returns all referenced table names in the given Query text.
// The returned format is `schemaName.tableName`.
func CollectTableNamesFromSQL(defaultSchemaName, sqlText string) (Set[TableName], error) {
	node, err := ParseOneSQL(sqlText)
	if err != nil {
		return nil, err
	}
	c := &tableNameCollector{defaultSchemaName: defaultSchemaName, tableNames: NewSet[TableName]()}
	node.Accept(c)
	return c.tableNames, nil
}

// IsTiDBSystemTableName returns whether the given table name is a TiDB system table name.
func IsTiDBSystemTableName(t TableName) bool {
	schemaName := strings.ToLower(t.SchemaName)
	return schemaName == "information_schema" ||
		schemaName == "metrics_schema" ||
		schemaName == "performance_schema" ||
		schemaName == "mysql"
}
