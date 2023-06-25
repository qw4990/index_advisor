package utils

import (
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
	tableNames Set[LowerString]
}

func (c *tableNameCollector) Enter(n ast.Node) (out ast.Node, skipChildren bool) {
	switch x := n.(type) {
	case *ast.TableName:
		c.tableNames.Add(LowerString(x.Name.String()))
	}
	return n, false
}

func (c *tableNameCollector) Leave(n ast.Node) (out ast.Node, ok bool) {
	return n, true
}

// CollectTableNamesFromSQL returns all referenced table names in the given SQL text.
func CollectTableNamesFromSQL(sqlText string) Set[LowerString] {
	node, err := ParseOneSQL(sqlText)
	Must(err)
	c := &tableNameCollector{tableNames: NewSet[LowerString]()}
	node.Accept(c)
	return c.tableNames
}
