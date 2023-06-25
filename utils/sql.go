package utils

import (
	"fmt"
	"github.com/pingcap/parser"
	"github.com/pingcap/parser/ast"
)

// ParseOneSQL parses the given SQL text and returns the AST.
func ParseOneSQL(sqlText string) (ast.StmtNode, error) {
	p := parser.New()
	return p.ParseOneStmt(sqlText, "", "")
}

// NormalizeDigest normalizes the given SQL text and returns the normalized SQL text and its digest.
func NormalizeDigest(sqlText string) (string, string) {
	return parser.NormalizeDigest(sqlText)
}

type tableNameCollector struct {
	defaultSchemaName string
	tableNames        Set[LowerString]
}

func (c *tableNameCollector) Enter(n ast.Node) (out ast.Node, skipChildren bool) {
	switch x := n.(type) {
	case *ast.TableName:
		if x.Schema.L == "" {
			c.tableNames.Add(LowerString(fmt.Sprintf("%s.%s", c.defaultSchemaName, x.Name.String())))
		} else {
			c.tableNames.Add(LowerString(fmt.Sprintf("%s.%s", x.Schema.L, x.Name.String())))
		}
	}
	return n, false
}

func (c *tableNameCollector) Leave(n ast.Node) (out ast.Node, ok bool) {
	return n, true
}

// CollectTableNamesFromSQL returns all referenced table names in the given SQL text.
// The returned format is `schemaName.tableName`.
func CollectTableNamesFromSQL(defaultSchemaName, sqlText string) Set[LowerString] {
	node, err := ParseOneSQL(sqlText)
	Must(err)
	c := &tableNameCollector{defaultSchemaName: defaultSchemaName, tableNames: NewSet[LowerString]()}
	node.Accept(c)
	return c.tableNames
}
