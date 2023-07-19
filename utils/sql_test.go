package utils

import (
	"sort"
	"strings"
	"testing"
)

func TestParseDNFColumnsFromQuery(t *testing.T) {
	cases := []struct {
		q string
		c []string
	}{
		{`select * from t where a = 1 or b = 2 or 3=c`,
			[]string{"test.t.a", "test.t.b", "test.t.c"}},
		{`select * from t where a = 1 or b = 2`,
			[]string{"test.t.a", "test.t.b"}},
		{`select * from t where a = 1`,
			[]string{}},
		{`select * from t where a = 1 and b =1`,
			[]string{}},
		{`select * from t where a = 1 and (b =1 or c=1)`,
			[]string{"test.t.b", "test.t.c"}},
		{`select * from t1, t2 where b =1 or c=1`,
			[]string{}}, // unsupported
		{
			`SELECT * FROM t WHERE
  				timestamp >= 1647469098 AND timestamp <= 1679005097
  				AND ( from_address = "eth:aaa" OR to_address = "eth:bbb" )
  				AND from_address <> to_address
				ORDER BY timestamp DESC LIMIT 200`,
			[]string{"test.t.from_address", "test.t.to_address"},
		},
		{
			`SELECT * FROM t WHERE
				  1 = 1
				  AND ( from_address = "aaa" OR to_address = "bbb" )
				  AND timestamp <= 1676423335681
				LIMIT 9007199254740991`,
			[]string{"test.t.from_address", "test.t.to_address"},
		},
	}

	for _, c := range cases {
		result, err := ParseDNFColumnsFromQuery(Query{
			SchemaName: "test",
			Text:       c.q,
		})
		must(err)
		checkDNFColResult(result, c.c)
	}
}

func checkDNFColResult(got Set[Column], want []string) {
	var gotStr []string
	if got != nil {
		for _, c := range got.ToList() {
			gotStr = append(gotStr, c.Key())
		}
	}
	sort.Strings(gotStr)
	sort.Strings(want)

	kGot := strings.Join(gotStr, ",")
	kWant := strings.Join(want, ",")
	if kGot != kWant {
		panic("got: " + kGot + ", want: " + kWant)
	}
}

func must(err error) {
	if err != nil {
		panic(err)
	}
}
