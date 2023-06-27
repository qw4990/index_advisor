# Index Advisor

## Introduction

Index Selection is an important part of database performance tuning. However, it is a complex and time-consuming task. Even experienced experts can hardly guarantee to find the best index set accurately and quickly when facing a complex workload containing dozens or even hundreds of tables and thousands of SQLs.

Index Advisor is a tool that can automatically recommend indexes based on the workload, statistics, and plan cost in TiDB, which can greatly reduce the workload of index selection in performance tuning.

## How it works

Index Advisor is based on the Hypo Index feature of TiDB. This feature allows users to create and maintain a series of hypothetical indexes in the optimizer. These indexes are only maintained in the optimizer and will not be actually created, so the overhead is very low. Combined with the `Explain` statement, you can evaluate the impact of an index on the query plan, and then determine whether the index is valuable.

The mechanism of Index Advisor is as follows, which can be roughly divided into three steps: 

![overview.png](doc/overview.png)

1. Index Advisor collects table structures, statistics, and related queries from the system table `information_schema.statements_summary` in TiDB.
2. Index Advisor generates a series of candidate indexes based on the collected information, and uses Hypo Index to maintain these indexes.
3. Index Advisor uses `Explain` to evaluate the value of these indexes (whether they can reduce plan costs) and make recommendations.

## How to use it

Index Advisor provides two ways to use it, online mode and offline mode:
1. In online mode, Index Advisor will directly access your TiDB instance for index analysis and recommendation.
2. In offline mode, Index Advisor will not access the TiDB instance. You need to manually prepare the information required by Index Advisor. Index Advisor will start a TiDB instance locally and then perform index analysis and recommendation.

The online mode is more convenient to use, and the offline mode is more flexible.

### Offline mode

In offline mode, you need to prepare the data required by Index Advisor in advance, including:
- A query file (or a folder): can be in the form of a single file or a folder.
    - A folder: such as `examples/tpch_example1/queries`, a folder, each file inside is a query.
    - A single file: such as `examples/tpch_example2/queries.sql`, which contains multiple query statements separated by semicolons.
- Schema information file (optional): such as `examples/tpch_example1/schema.sql`, which contains the original `create-table` statements separated by semicolons.
- Statistics information folder (optional): such as `examples/tpch_example1/stats`, a folder, which stores the statistics information files of related tables. Each statistics information file should be in JSON format and can be downloaded through the TiDB statistics information dump.

After preparing the above files, you can directly use Index Advisor for index recommendation, such as `index_advisor --offline --query-path=examples/tpch_example1/queries --max-num-indexes=5`, where the parameters are:
- `offline`: indicates offline mode.
- `query-path`: the path of the query file, which can be a single file or a folder.
- `schema-path`: the path of the schema information file, optional; if specified, the table will be created using this file.
- `stats-path`: the path of the statistics information folder, optional; if specified, the statistics information in the folder will be imported.
- `max-num-indexes`: the maximum number of indexes recommended.
- `cost-model-version`: the cost model version used by TiDB, see [TiDB Cost Model Version](https://docs.pingcap.com/tidb/dev/system-variables#tidb_cost_model_version-starting-from-v620-version).
- `output`: the path to save the output results, optional; if empty, the results will be printed directly on the terminal.

### Online mode

In online mode, you need to ensure that the following conditions are met:
- Please make sure that your TiDB version is higher than v6.5.x or v7.1.x, or higher than v7.2, to use the Hypo Index feature.
- Please make sure that the `Statement Summary` feature is enabled on your TiDB by default. Index Advisor needs to obtain query information from this system table.
- You need to turn off the `tidb_redact_log` feature, otherwise Index Advisor cannot get the original text of the query from `Statement Summary`.
- Use Index Advisor for index recommendation, such as `index_advisor --online --dsn='user1:@tcp(127.0.0.1:4000)' --max-num-indexes=5 --query-exec-time-threshold=300ms`:
    - `online`: indicates online mode.
    - `dsn`: the DSN to access your TiDB instance.
    - `max-num-indexes`: the maximum number of indexes recommended.
    - `query-exec-time-threshold`: only recommend indexes for queries whose execution time exceeds this threshold. 

Index Advisor will output the recommended indexes and the benefits of the corresponding queries. You can create new indexes based on the output results.

## Output

The output of Index Advisor is divided into two parts: recommended indexes and benefit evaluation

```
============== Recommended Indexes ==============
CREATE INDEX t_uid_oid ON t (uid, oid);
...

============== Benefit Evaluation ==============
Total query plan cost: 100000 -> 30000, improvement: 70.00%
Q1 plan cost: 1000 -> 100, improvement: 90.00%
Q2 plan cost: 1000 -> 100, improvement: 90.00%
...
```

## Evaluation

We evaluated the performance of Index Advisor on multiple benchmarks. 

### TPC-H

We use TPC-H 1G for evaluation, which contains 8 tables and 21 queries (excluding q15), and let Index Advisor recommend 5 indexes for these queries.

After creating these 5 recommended indexes, the total execution time is reduced from 32.86s to 26.61s, a decrease of nearly 20%:

![tpch_total](doc/evaluation_tpch_1g_total.png)

Below are queries that can gain significant performance improvements:

![tpch_query](doc/evaluation_tpch_1g_query.png)

### JOB

TODO

### TPC-DS

TODO

### TODO

## Usages

TODO