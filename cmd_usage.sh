./index_advisor advise-offline --query-path='./examples/tpch_example1/queries' --schema-path='./examples/tpch_example1/schema.sql' --stats-path='./examples/tpch_example1/stats' --dsn='root:@tcp(127.0.0.1:4000)/tpch' --output='./data/output' --max-num-indexes=1

./index_advisor advise-offline --query-path='./examples/tpch_example1/queries' --schema-path='./examples/tpch_example1/schema.sql' --stats-path='./examples/tpch_example1/stats' --dsn='root:@tcp(127.0.0.1:4000)/tpch' --output='./data/output' --max-num-indexes=3

./index_advisor advise-offline --query-path='./examples/tpch_example1/queries' --schema-path='./examples/tpch_example1/schema.sql' --stats-path='./examples/tpch_example1/stats' --dsn='root:@tcp(127.0.0.1:4000)/tpch' --output='./data/output' --max-num-indexes=5

./index_advisor advise-offline --query-path='./examples/job_no_fk/queries' --schema-path='./examples/job_no_fk/schema.sql' --stats-path='./examples/job_no_fk/stats' --dsn='root:@tcp(127.0.0.1:4000)/imdbload_no_fk' --output='./data/output' --max-num-indexes=8