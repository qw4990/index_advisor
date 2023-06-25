package cmd

import (
	"sort"
	"testing"

	"github.com/qw4990/index_advisor/optimizer"
	"github.com/qw4990/index_advisor/utils"
)

func TestReadQueries(t *testing.T) {
	dsn := "root:@tcp(127.0.0.1:4000)/test"
	db, err := optimizer.NewTiDBWhatIfOptimizer(dsn)
	must(err)

	must(db.Execute(`drop database if exists read_queries_test`))
	must(db.Execute(`create database read_queries_test`))
	defer func() {
		must(db.Execute(`drop database if exists read_queries_test`))
	}()
	must(db.Execute(`use read_queries_test`))
	must(db.Execute(`create table t1 (a int)`))

	queries := []string{
		`select * from t1`,
		`select * from t1 where a in (1, 2, 3)`,
		`select * from t1 where a > 10`,
	}
	for _, q := range queries {
		must(db.Execute(q))
	}
	// Queries below should be ignored
	must(db.Execute(`select * from information_schema.statements_summary`))
	must(db.Execute(`use mysql`))
	must(db.Execute(`select * from bind_info`))
	sqls := readQueriesFromStatementSummary(db, []string{"read_queries_test"})
	sqls = filterSQLAccessingSystemTables(sqls)
	if sqls.Size() != len(queries) {
		t.Fatalf("expect %+v, got %+v", queries, sqls)
	}
	for _, q := range queries {
		if !sqls.Contains(utils.Query{Text: q}) {
			t.Fatalf("expect %+v, got %+v", queries, sqls)
		}
	}
}

func TestReadTableSchemas(t *testing.T) {
	dsn := "root:@tcp(127.0.0.1:4000)/test"
	db, err := optimizer.NewTiDBWhatIfOptimizer(dsn)
	must(err)

	must(db.Execute(`drop database if exists read_table_name_test`))
	must(db.Execute(`create database read_table_name_test`))
	defer func() {
		must(db.Execute(`drop database if exists read_table_name_test`))
	}()
	must(db.Execute(`use read_table_name_test`))
	must(db.Execute(`create table t1 (a int)`))
	must(db.Execute(`create table t2 (a int)`))
	must(db.Execute(`create table t3 (a int)`))

	schemas := readTableSchemas(db, []string{`read_table_name_test`})
	if !schemas.ContainsKey("read_table_name_test.t1") ||
		!schemas.ContainsKey("read_table_name_test.t2") ||
		!schemas.ContainsKey("read_table_name_test.t3") {
		t.Fatalf("expect t1, t2, t3, got %+v", schemas)
	}
}

func TestReadTableNames(t *testing.T) {
	dsn := "root:@tcp(127.0.0.1:4000)/test"
	db, err := optimizer.NewTiDBWhatIfOptimizer(dsn)
	must(err)

	must(db.Execute(`drop database if exists read_table_name_test`))
	must(db.Execute(`create database read_table_name_test`))
	defer func() {
		must(db.Execute(`drop database if exists read_table_name_test`))
	}()
	must(db.Execute(`use read_table_name_test`))
	must(db.Execute(`create table t1 (a int)`))
	must(db.Execute(`create table t2 (a int)`))
	must(db.Execute(`create table t3 (a int)`))

	names := readTableNames(db, "read_table_name_test")
	sort.Strings(names)
	if len(names) != 3 || names[0] != "t1" || names[1] != "t2" || names[2] != "t3" {
		t.Fatalf("expect t1, t2, t3, got %+v", names)
	}
}

func must(err error) {
	if err != nil {
		panic(err)
	}
}
