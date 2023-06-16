package advisor

import (
	"fmt"
	"github.com/qw4990/index_advisor/utils"
	"github.com/qw4990/index_advisor/workload"
	"testing"
)

func TestFindIndexableColumnsSimple(t *testing.T) {
	workload := workload.WorkloadInfo{
		TableSchemas: utils.ListToSet(workload.TableSchema{"test", "t", workload.NewColumns("test", "t", "a", "b", "c", "d", "e"), nil, ""}),
		SQLs: utils.ListToSet(workload.SQL{"", "test", "select * from t where a<1 and b>1 and e like 'abc'", 1, nil, nil},
			workload.SQL{"", "test", "select * from t where c in (1, 2, 3) order by d", 1, nil, nil}),
	}
	utils.Must(IndexableColumnsSelectionSimple(&workload))
	fmt.Println(workload.IndexableColumns.ToList())
	for _, sql := range workload.SQLs.ToList() {
		fmt.Println(sql.IndexableColumns.ToList())
	}
}

func TestFindIndexableColumnsSimple2(t *testing.T) {
	t1, err := workload.ParseCreateTableStmt("test", "create table t1 (a int)")
	utils.Must(err)
	t2, err := workload.ParseCreateTableStmt("test", "create table t2 (a int)")
	utils.Must(err)
	workload := workload.WorkloadInfo{
		TableSchemas: utils.ListToSet(t1, t2),
		SQLs:         utils.ListToSet(workload.SQL{"", "test", "select * from t2 tx where a<1", 1, nil, nil}),
	}
	utils.Must(IndexableColumnsSelectionSimple(&workload))
	fmt.Println(workload.IndexableColumns.ToList())
}

func TestFindIndexableColumnsSimpleJOB(t *testing.T) {
	w, err := workload.LoadWorkloadInfo("imdbload", "./workload/job")
	utils.Must(err)
	w.SQLs = workload.FilterBySQLAlias(w.SQLs, []string{"1a"})
	utils.Must(IndexableColumnsSelectionSimple(&w))
	for _, c := range w.IndexableColumns.ToList() {
		fmt.Println(c)
	}
}

func TestFindIndexableColumnsSimpleTPCH(t *testing.T) {
	workload := workload.WorkloadInfo{
		TableSchemas: utils.ListToSet(
			workload.TableSchema{"tpch", "nation", workload.NewColumns("tpch", "nation", "N_NATIONKEY", "N_NAME", "N_REGIONKEY", "N_COMMENT"), nil, ""}),
		SQLs: utils.ListToSet(
			workload.SQL{"", "tpch", `select
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
	l_year`, 1, nil, nil})}
	utils.Must(IndexableColumnsSelectionSimple(&workload))
	fmt.Println(workload.IndexableColumns.ToList())
	for _, sql := range workload.SQLs.ToList() {
		fmt.Println(sql.IndexableColumns.ToList())
	}
}
