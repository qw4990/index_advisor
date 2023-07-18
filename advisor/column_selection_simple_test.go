package advisor

import (
	"fmt"
	"sort"
	"strings"
	"testing"

	"github.com/qw4990/index_advisor/utils"
)

func must(err error) {
	if err != nil {
		panic(err)
	}
}

func TestFindIndexableColumnsCase1(t *testing.T) {
	workload := utils.WorkloadInfo{
		TableSchemas: utils.ListToSet(utils.TableSchema{"test", "t", utils.NewColumns("test", "t", "a", "b", "c", "d", "e"), nil, ""}),
		Queries: utils.ListToSet(utils.Query{"", "test", "select * from t where a<1 and b>1 and e like 'abc'", 1, nil},
			utils.Query{"", "test", "select * from t where c in (1, 2, 3) order by d", 1, nil}),
	}
	must(IndexableColumnsSelectionSimple(&workload))
	fmt.Println(workload.IndexableColumns.ToList())
	for _, sql := range workload.Queries.ToList() {
		fmt.Println(sql.IndexableColumns.ToList())
	}
}

func TestFindIndexableColumnsCase2(t *testing.T) {
	t1, err := utils.ParseCreateTableStmt("test", "create table t1 (a int)")
	must(err)
	t2, err := utils.ParseCreateTableStmt("test", "create table t2 (a int)")
	must(err)
	workload := utils.WorkloadInfo{
		TableSchemas: utils.ListToSet(t1, t2),
		Queries:      utils.ListToSet(utils.Query{"", "test", "select * from t2 tx where a<1", 1, nil}),
	}
	must(IndexableColumnsSelectionSimple(&workload))
	fmt.Println(workload.IndexableColumns.ToList())
}

func TestFindIndexableColumnsCase3(t *testing.T) {
	t1, err := utils.ParseCreateTableStmt("db1", "create table t1 (a1 int)")
	must(err)
	t2, err := utils.ParseCreateTableStmt("db2", "create table t2 (a2 int)")
	must(err)
	workload := utils.WorkloadInfo{
		TableSchemas: utils.ListToSet(t1, t2),
		Queries: utils.ListToSet(utils.Query{"", "db1",
			"select * from db2.t2 where a2<1", 1, nil}),
	}
	must(IndexableColumnsSelectionSimple(&workload))
	checkIndexableCols(workload.IndexableColumns, []string{"db2.t2.a2"})
}

func TestFindIndexableColumnsSimpleTPCH(t *testing.T) {
	workload := utils.WorkloadInfo{
		TableSchemas: utils.ListToSet(
			utils.TableSchema{"tpch", "nation", utils.NewColumns("tpch", "nation", "N_NATIONKEY", "N_NAME", "N_REGIONKEY", "N_COMMENT"), nil, ""}),
		Queries: utils.ListToSet(
			utils.Query{"", "tpch", `select
	supp_nation,
	cust_nation,
	l_year,
	sum(volume) as revenue
from
	(
		select
			n1.n_name as supp_nation,
			n2.n_name as cust_nation,
			extract(year from l_shipdate) as l_year,
			l_extendedprice * (1 - l_discount) as volume
		from
			supplier,
			lineitem,
			orders,
			customer,
			nation n1,
			nation n2
		where
			s_suppkey = l_suppkey
			and o_orderkey = l_orderkey
			and c_custkey = o_custkey
			and s_nationkey = n1.n_nationkey
			and c_nationkey = n2.n_nationkey
			and (
				(n1.n_name = 'MOZAMBIQUE' and n2.n_name = 'UNITED KINGDOM')
				or (n1.n_name = 'UNITED KINGDOM' and n2.n_name = 'MOZAMBIQUE')
			)
			and l_shipdate between date '1995-01-01' and date '1996-12-31'
	) as shipping
group by
	supp_nation,
	cust_nation,
	l_year
order by
	supp_nation,
	cust_nation,
	l_year`, 1, nil})}
	must(IndexableColumnsSelectionSimple(&workload))
	fmt.Println(workload.IndexableColumns.ToList())
	for _, sql := range workload.Queries.ToList() {
		fmt.Println(sql.IndexableColumns.ToList())
	}
}

func checkIndexableCols(got utils.Set[utils.Column], expected []string) {
	var gotCols []string
	for _, c := range got.ToList() {
		gotCols = append(gotCols, c.Key())
	}
	sort.Strings(gotCols)
	sort.Strings(expected)
	gotStr := strings.Join(gotCols, ",\n")
	expectedStr := strings.Join(expected, ",\n")
	if gotStr != expectedStr {
		panic(fmt.Sprintf("got %s, expected %s", gotStr, expectedStr))
	}
}
