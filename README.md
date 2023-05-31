# index_advisor

Examples: 
```
➜  ./index_advisor advise --workload-info-path='./workload/tpch_1g_22' --schema-name='tpch'

[DEBUG] loading workload info from ./workload/tpch_1g_22
[DEBUG] starting index advise with compress algorithm none, indexable algorithm simple, index selection algorithm auto_admin
[DEBUG] connecting to root:@tcp(127.0.0.1:4000)/test
[DEBUG] compressing workload info from 21 SQLs to 21 SQLs
[DEBUG] finding 45 indexable columns
[DEBUG] starting auto-admin algorithm with max-indexes 10, max index-width 3, max index-naive 2
...
[DEBUG] what-if optimizer stats: Execute(count/time): (114242/14.129283356s), CreateOrDropHypoIndex: (73292/10.767289324s), GetCost: (40950/32.182477129s)
CREATE INDEX idx_c_nationkey ON tpch.customer (c_nationkey)
CREATE INDEX idx_o_custkey_o_orderdate_o_shippriority ON tpch.orders (o_custkey, o_orderdate, o_shippriority)
CREATE INDEX idx_l_partkey_l_quantity ON tpch.lineitem (l_partkey, l_quantity)
CREATE INDEX idx_l_shipdate ON tpch.lineitem (l_shipdate)
CREATE INDEX idx_o_custkey_o_orderdate_o_orderpriority ON tpch.orders (o_custkey, o_orderdate, o_orderpriority)
CREATE INDEX idx_l_shipmode_l_partkey ON tpch.lineitem (l_shipmode, l_partkey)
CREATE INDEX idx_p_size_p_type_p_brand ON tpch.part (p_size, p_type, p_brand)
CREATE INDEX idx_l_suppkey_l_shipdate ON tpch.lineitem (l_suppkey, l_shipdate)
CREATE INDEX idx_ps_suppkey_ps_supplycost ON tpch.partsupp (ps_suppkey, ps_supplycost)
CREATE INDEX idx_l_partkey_l_shipdate ON tpch.lineitem (l_partkey, l_shipdate)
CREATE INDEX idx_o_orderdate_o_custkey_o_totalprice ON tpch.orders (o_orderdate, o_custkey, o_totalprice)
original workload cost: 2.60E+09
optimized workload cost: 1.23E+09

```