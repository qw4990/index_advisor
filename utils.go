package main

import (
	"fmt"
	"os"
	"path"
	"sort"
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

func NewWorkloadFromStmt(schemaName string, createTableStmts, rawSQLs []string) WorkloadInfo {
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
// TODO: for simplification, assume all SQLs are under the same schema here.
func LoadWorkloadInfo(schemaName, workloadInfoPath string) (WorkloadInfo, error) {
	Debugf("loading workload info from %s", workloadInfoPath)
	sqlFilePath := path.Join(workloadInfoPath, "sqls.sql")
	rawSQLs, err := ParseRawSQLsFromFile(sqlFilePath)
	must(err, workloadInfoPath)
	sqls := NewSet[SQL]()
	for i, rawSQL := range rawSQLs {
		sqls.Add(SQL{
			Alias:      fmt.Sprintf("q%v", i+1),
			SchemaName: schemaName,
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

func ParseCreateTableStmt(schemaName, createTableStmt string) (TableSchema, error) {
	stmt, err := ParseOneSQL(createTableStmt)
	must(err, createTableStmt)
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
		})
	}
	// TODO: parse indexes
	return t, nil
}

func ParseOneSQL(sqlText string) (ast.StmtNode, error) {
	p := parser.New()
	return p.ParseOneStmt(sqlText, "", "")
}

func evaluateIndexConfCost(info WorkloadInfo, optimizer WhatIfOptimizer, indexes Set[Index]) IndexConfCost {
	for _, index := range indexes.ToList() {
		must(optimizer.CreateHypoIndex(index))
	}
	cost := workloadQueryCost(info, optimizer)
	for _, index := range indexes.ToList() {
		must(optimizer.DropHypoIndex(index))
	}
	var totCols int
	for _, index := range indexes.ToList() {
		totCols += len(index.Columns)
	}
	return IndexConfCost{cost, totCols}
}

func workloadQueryCost(info WorkloadInfo, optimizer WhatIfOptimizer) float64 {
	var workloadCost float64
	for _, sql := range info.SQLs.ToList() { // TODO: run them concurrently to save time
		if sql.Type() != SQLTypeSelect {
			continue
		}
		must(optimizer.Execute(`use ` + sql.SchemaName))
		p, err := optimizer.Explain(sql.Text)
		must(err, sql.Text)
		workloadCost += p.PlanCost() * float64(sql.Frequency)
	}
	return workloadCost
}

// TempIndexName returns a temp index name for the given columns.
func TempIndexName(cols ...Column) string {
	var names []string
	for _, col := range cols {
		names = append(names, col.ColumnName)
	}
	return fmt.Sprintf("idx_%v", strings.Join(names, "_"))
}

type SetKey interface {
	Key() string
}

type Set[T SetKey] interface {
	Add(item T)
	AddList(items ...T)
	AddSet(set Set[T])
	Contains(item T) bool
	Find(k SetKey) (T, bool)
	Remove(item T)
	ToList() []T
	Size() int
	Clone() Set[T]
	String() string
}

type setImpl[T SetKey] struct {
	s map[string]T
}

func NewSet[T SetKey]() Set[T] {
	return new(setImpl[T])
}

func (s *setImpl[T]) Add(item T) {
	if s.s == nil {
		s.s = make(map[string]T)
	}
	s.s[item.Key()] = item
}

func (s *setImpl[T]) Contains(item T) bool {
	if s.s == nil {
		return false
	}
	_, ok := s.s[item.Key()]
	return ok
}

func (s *setImpl[T]) Find(k SetKey) (a T, ok bool) {
	if s.s == nil {
		return a, false
	}
	v, ok := s.s[k.Key()]
	return v, ok
}

func (s *setImpl[T]) ToList() []T {
	var list []T
	for _, v := range s.s {
		list = append(list, v)
	}
	return list
}

func (s *setImpl[T]) AddList(items ...T) {
	for _, item := range items {
		s.Add(item)
	}
}

func (s *setImpl[T]) AddSet(set Set[T]) {
	s.AddList(set.ToList()...)
}

func (s *setImpl[T]) Remove(item T) {
	delete(s.s, item.Key())
}

func (s *setImpl[T]) Size() int {
	return len(s.s)
}

func (s *setImpl[T]) Clone() Set[T] {
	clone := NewSet[T]()
	clone.AddSet(s)
	return clone
}

func (s *setImpl[T]) String() string {
	var items []string
	for _, item := range s.s {
		items = append(items, item.Key())
	}
	sort.Strings(items)
	return fmt.Sprintf("{%v}", strings.Join(items, ", "))
}

func ListToSet[T SetKey](items ...T) Set[T] {
	s := NewSet[T]()
	s.AddList(items...)
	return s
}

func UnionSet[T SetKey](ss ...Set[T]) Set[T] {
	if len(ss) == 0 {
		return NewSet[T]()
	}
	if len(ss) == 1 {
		return ss[0].Clone()
	}
	s := NewSet[T]()
	for _, set := range ss {
		s.AddSet(set)
	}
	return s
}

func AndSet[T SetKey](ss ...Set[T]) Set[T] {
	if len(ss) == 0 {
		return NewSet[T]()
	}
	if len(ss) == 1 {
		return ss[0].Clone()
	}
	s := NewSet[T]()
	for _, item := range ss[0].ToList() {
		contained := true
		for _, set := range ss[1:] {
			if !set.Contains(item) {
				contained = false
				break
			}
		}
		if contained {
			s.Add(item)
		}
	}
	return s
}

func DiffSet[T SetKey](s1, s2 Set[T]) Set[T] {
	s := NewSet[T]()
	for _, item := range s1.ToList() {
		if !s2.Contains(item) {
			s.Add(item)
		}
	}
	return s
}

func CombSet[T SetKey](s Set[T], numberOfItems int) []Set[T] {
	return combSetIterate(s.ToList(), NewSet[T](), 0, numberOfItems)
}

func combSetIterate[T SetKey](itemList []T, currSet Set[T], depth, numberOfItems int) []Set[T] {
	if currSet.Size() == numberOfItems {
		return []Set[T]{currSet.Clone()}
	}
	if depth == len(itemList) || currSet.Size() > numberOfItems {
		return nil
	}
	var res []Set[T]
	currSet.Add(itemList[depth])
	res = append(res, combSetIterate(itemList, currSet, depth+1, numberOfItems)...)
	currSet.Remove(itemList[depth])
	res = append(res, combSetIterate(itemList, currSet, depth+1, numberOfItems)...)
	return res
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

// checkWorkloadInfo checks whether this workload info is fulfilled.
func checkWorkloadInfo(w WorkloadInfo) {
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
			if col.SchemaName == "" || col.TableName == "" || col.ColumnName == "" {
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
