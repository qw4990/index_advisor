package main

import (
	"fmt"
	"os"
	"path"
	"strings"

	"github.com/pingcap/parser"
	"github.com/pingcap/parser/ast"
)

func must(err error, args ...interface{}) {
	if err != nil {
		fmt.Println("panic args: ", args)
		panic(err)
	}
}

func isTrue(floag bool, args ...interface{}) {
	if !floag {
		fmt.Println("panic args: ", args)
		panic("not true")
	}
}

func saveContentTo(fpath, content string) {
	must(os.WriteFile(fpath, []byte(content), 0644))
}

// FileExists tests whether this file exists and is or not a directory.
func FileExists(filename string) (exist, isDir bool) {
	info, err := os.Stat(filename)
	if os.IsNotExist(err) {
		return false, false
	}
	return true, info.IsDir()
}

// ParseRawSQLsFromDir parses raw SQLs from the given directory.
// Each *.sql in this directory is parsed as a single SQL.
func ParseRawSQLsFromDir(dirPath string) (sqls, fileNames []string, err error) {
	des, err := os.ReadDir(dirPath)
	if err != nil {
		return nil, nil, err
	}
	for _, entry := range des {
		if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".sql") {
			continue
		}
		fpath := path.Join(dirPath, entry.Name())
		content, err := os.ReadFile(fpath)
		if err != nil {
			return nil, nil, err
		}
		sql := strings.TrimSpace(string(content))
		sqls = append(sqls, sql)
		fileNames = append(fileNames, entry.Name())
	}
	return
}

// ParseRawSQLsFromFile parses raw SQLs from the given file.
// It ignore all comments, and assume all SQLs are separated by ';'.
func ParseRawSQLsFromFile(fpath string) ([]string, error) {
	data, err := os.ReadFile(fpath)
	if err != nil {
		return nil, err
	}
	lines := strings.Split(string(data), "\n")
	var filteredLines []string
	for _, line := range lines {
		line = strings.TrimSpace(line)
		if line == "" || strings.HasPrefix(line, "--") { // empty line or comment
			continue
		}
		filteredLines = append(filteredLines, line)
	}
	content := strings.Join(filteredLines, "\n")

	tmp := strings.Split(content, ";")
	var sqls []string
	for _, sql := range tmp {
		sql = strings.TrimSpace(sql)
		if sql == "" {
			continue
		}
		sqls = append(sqls, sql)
	}
	return sqls, nil
}

// ParseOneSQL parses the given SQL text and returns the AST.
func ParseOneSQL(sqlText string) (ast.StmtNode, error) {
	p := parser.New()
	return p.ParseOneStmt(sqlText, "", "")
}

func filterBySQLAlias(sqls Set[SQL], alias []string) Set[SQL] {
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

func CollectTableNamesFromSQL(sqlText string) Set[LowerString] {
	node, err := ParseOneSQL(sqlText)
	must(err)
	c := &tableNameCollector{tableNames: NewSet[LowerString]()}
	node.Accept(c)
	return c.tableNames
}

func min[T int | float64](xs ...T) T {
	res := xs[0]
	for _, x := range xs {
		if x < res {
			res = x
		}
	}
	return res
}
