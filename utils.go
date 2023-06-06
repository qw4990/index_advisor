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
