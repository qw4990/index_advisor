load data local infile '/Users/zhangyuanjia/Workspace/go/src/github.com/qw4990/index_advisor/workload/tpch_100m_22/data/region.tbl' into table tpch.region fields terminated by '|';
load data local infile '/Users/zhangyuanjia/Workspace/go/src/github.com/qw4990/index_advisor/workload/tpch_100m_22/data/nation.tbl' into table tpch.nation fields terminated by '|';
load data local infile '/Users/zhangyuanjia/Workspace/go/src/github.com/qw4990/index_advisor/workload/tpch_100m_22/data/supplier.tbl' into table tpch.supplier fields terminated by '|';
load data local infile '/Users/zhangyuanjia/Workspace/go/src/github.com/qw4990/index_advisor/workload/tpch_100m_22/data/customer.tbl' into table tpch.customer fields terminated by '|';
load data local infile '/Users/zhangyuanjia/Workspace/go/src/github.com/qw4990/index_advisor/workload/tpch_100m_22/data/part.tbl' into table tpch.part fields terminated by '|';
load data local infile '/Users/zhangyuanjia/Workspace/go/src/github.com/qw4990/index_advisor/workload/tpch_100m_22/data/partsupp.tbl' into table tpch.partsupp fields terminated by '|';
load data local infile '/Users/zhangyuanjia/Workspace/go/src/github.com/qw4990/index_advisor/workload/tpch_100m_22/data/orders.tbl' into table tpch.orders fields terminated by '|';
load data local infile '/Users/zhangyuanjia/Workspace/go/src/github.com/qw4990/index_advisor/workload/tpch_100m_22/data/lineitem.tbl' into table tpch.lineitem fields terminated by '|';
    
analyze table tpch.region;
analyze table tpch.nation;
analyze table tpch.supplier;
analyze table tpch.customer;
analyze table tpch.part;
analyze table tpch.partsupp;
analyze table tpch.orders;
analyze table tpch.lineitem;