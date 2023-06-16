package utils

import (
	"fmt"
	"testing"
)

func TestLoadWorkloadInfo(t *testing.T) {
	w, err := LoadWorkloadInfo("test", "./workload/test")
	must(err)
	isTrue(w.SQLs.Size() == 8)
}

func TestLoadWorkloadInfoTPCH(t *testing.T) {
	w, err := LoadWorkloadInfo("tpch", "./workload/tpch_1g_22")
	must(err)
	isTrue(w.SQLs.Size() == 21)
	fmt.Println(w.SQLs.Size())
}

func TestLoadWorkloadJOB(t *testing.T) {
	w, err := LoadWorkloadInfo("imdbload", "./workload/job")
	must(err)
	isTrue(w.SQLs.Size() == 113)
}

func TestCollectTableNames(t *testing.T) {
	sql := `
SELECT MIN(mc.note) AS production_note, MIN(t.title) AS movie_title
	, MIN(t.production_year) AS movie_year
FROM company_type ct, info_type it, movie_companies mc, movie_info_idx mi_idx, title t
WHERE ct.kind = 'production companies'
	AND it.info = 'top 250 rank'
	AND mc.note NOT LIKE '%(as Metro-Goldwyn-Mayer Pictures)%'
	AND (mc.note LIKE '%(co-production)%'
		OR mc.note LIKE '%(presents)%')
	AND ct.id = mc.company_type_id
	AND t.id = mc.movie_id
	AND t.id = mi_idx.movie_id
	AND mc.movie_id = mi_idx.movie_id
	AND it.id = mi_idx.info_type_id;
`
	tables := CollectTableNamesFromSQL(sql)
	fmt.Println(tables.ToList())
}

func TestCombSet(t *testing.T) {
	s := NewSet[Column]()
	for i := 0; i < 6; i++ {
		s.Add(NewColumn("test", "test", fmt.Sprintf("col%d", i)))
	}

	for i := 1; i < 6; i++ {
		fmt.Println("======================== ", i, " ========================")
		result := CombSet(s, i)
		fmt.Println("--> ", len(result))
		for _, ss := range result {
			fmt.Println(ss.ToList())
		}
	}
}
