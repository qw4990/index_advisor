package cmd

import (
	"errors"
	"fmt"
	"strconv"
	"strings"

	"github.com/qw4990/index_advisor/advisor"
	"github.com/qw4990/index_advisor/optimizer"
	"github.com/qw4990/index_advisor/utils"
	"github.com/spf13/cobra"
)

type adviseOnlineCmdOpt struct {
	maxNumIndexes int
	maxIndexWidth int

	dsn      string
	output   string
	logLevel string

	querySchemas            []string
	queryExecTimeThreshold  int
	queryExecCountThreshold int
	queryPath               string
}

func NewAdviseOnlineCmd() *cobra.Command {
	var opt adviseOnlineCmdOpt
	cmd := &cobra.Command{
		Use:   "advise-online",
		Short: "advise some indexes for the specified workload",
		Long:  `advise some indexes for the specified workload`,
		RunE: func(cmd *cobra.Command, args []string) error {
			utils.SetLogLevel(opt.logLevel)

			db, err := optimizer.NewTiDBWhatIfOptimizer(opt.dsn)
			if err != nil {
				return err
			}

			if !supportHypoIndex(db) {
				return errors.New("your TiDB version does not support hypothetical indexes")
			}
			if redactLogEnabled(db) {
				utils.Warningf("redact log is enabled, the Advisor probably cannot get the full SQL text")
			}

			var queries utils.Set[utils.Query]
			_, dbName := utils.GetDBNameFromDSN(opt.dsn)
			if opt.queryPath != "" {
				queries, err = readQueriesFromStatementSummary(db, opt)
				if err != nil {
					return err
				}
			} else {
				queries, err = utils.LoadQueries(dbName, opt.queryPath)
				if err != nil {
					return err
				}
			}
			queries, err = filterSQLAccessingSystemTables(queries)
			if err != nil {
				return err
			}
			tableNames, err := utils.CollectTableNamesFromQueries(dbName, queries)
			if err != nil {
				return err
			}
			tables, err := getTableSchemas(db, tableNames)
			if err != nil {
				return err
			}
			info := utils.WorkloadInfo{
				Queries:      queries,
				TableSchemas: tables,
			}

			indexes, err := advisor.IndexAdvise(db, info, advisor.Parameter{
				MaxNumberIndexes: opt.maxNumIndexes,
				MaxIndexWidth:    opt.maxIndexWidth,
			})
			if err != nil {
				return err
			}
			return outputAdviseResult(indexes, info, db, opt.output)
		},
	}

	cmd.Flags().IntVar(&opt.maxNumIndexes, "max-num-indexes", 5, "max number of indexes to recommend, 1~20")
	cmd.Flags().IntVar(&opt.maxIndexWidth, "max-index-width", 3, "the max number of columns in recommended indexes")

	cmd.Flags().StringVar(&opt.dsn, "dsn", "root:@tcp(127.0.0.1:4000)/test", "dsn")
	cmd.Flags().StringVar(&opt.output, "output", "", "output directory to save the result")
	cmd.Flags().StringVar(&opt.logLevel, "log-level", "info", "log level, one of 'debug', 'info', 'warning', 'error'")

	cmd.Flags().StringSliceVar(&opt.querySchemas, "query-schemas", []string{}, "a list of schema(database), e.g. 'test1, test2', queries that are running under these schemas will be considered")
	cmd.Flags().IntVar(&opt.queryExecTimeThreshold, "query-exec-time-threshold", 0, "the threshold of query execution time(in milliseconds), e.g. '300', queries that are running longer than this threshold will be considered")
	cmd.Flags().IntVar(&opt.queryExecCountThreshold, "query-exec-count-threshold", 0, "the threshold of query execution count, e.g. '20', queries that are executed more than this threshold will be considered")
	cmd.Flags().StringVar(&opt.queryPath, "query-path", "", "the path that contains queries, e.g. 'queries.sql', if this variable is specified, the above variables like 'query-*' will be ignored")
	return cmd
}

func readQueriesFromStatementSummary(db optimizer.WhatIfOptimizer, opt adviseOnlineCmdOpt) (utils.Set[utils.Query], error) {
	var condition []string
	condition = append(condition, "stmt_type='Select'")
	if len(opt.querySchemas) == 0 {
		return nil, errors.New("query-schemas is required")
	}
	condition = append(condition, fmt.Sprintf("SCHEMA_NAME in ('%s')", strings.Join(opt.querySchemas, "', '")))
	if opt.queryExecTimeThreshold > 0 {
		condition = append(condition, fmt.Sprintf("AVG_LATENCY > %v", opt.queryExecTimeThreshold*1000))
	}
	if opt.queryExecCountThreshold > 0 {
		condition = append(condition, fmt.Sprintf("EXEC_COUNT > %v", opt.queryExecCountThreshold))
	}

	s := utils.NewSet[utils.Query]()
	for _, table := range []string{
		`information_schema.statements_summary`,
		`information_schema.statements_summary_history`,
	} {
		// TODO: consider Execute statements
		q := fmt.Sprintf(`select SCHEMA_NAME, DIGEST, QUERY_SAMPLE_TEXT, EXEC_COUNT, AVG_LATENCY from %v where %v`,
			table, strings.Join(condition, " AND "))
		rows, err := db.Query(q)
		if err != nil {
			return nil, err
		}
		for rows.Next() {
			var schemaName, digest, text, execCountStr, avgLatStr string
			if err := rows.Scan(&schemaName, &digest, &text, &execCountStr, &avgLatStr); err != nil {
				return nil, err
			}
			execCount, err := strconv.Atoi(execCountStr)
			if err != nil {
				return nil, err
			}
			s.Add(utils.Query{
				Alias:      digest,
				SchemaName: schemaName,
				Text:       text,
				Frequency:  execCount,
			})
		}
		if err := rows.Close(); err != nil {
			return nil, err
		}
	}
	return s, nil
}

func readTableSchemas(db optimizer.WhatIfOptimizer, schemas []string) (utils.Set[utils.TableSchema], error) {
	s := utils.NewSet[utils.TableSchema]()
	for _, schemaName := range schemas {
		tableNames, err := readTableNames(db, schemaName)
		if err != nil {
			return nil, err
		}
		for _, tableName := range tableNames {
			q := fmt.Sprintf(`show create table %s.%s`, schemaName, tableName)
			rows, err := db.Query(q)
			if err != nil {
				return nil, err
			}
			for rows.Next() {
				var name, createTableStmt string
				if err := rows.Scan(&name, &createTableStmt); err != nil {
					return nil, err
				}
				tableSchema, err := utils.ParseCreateTableStmt(schemaName, createTableStmt)
				if err != nil {
					return nil, err
				}
				s.Add(tableSchema)
			}
			rows.Close()
		}
	}
	return s, nil
}

func readTableNames(db optimizer.WhatIfOptimizer, schemaName string) ([]string, error) {
	if err := db.Execute(fmt.Sprintf(`use %s`, schemaName)); err != nil {
		return nil, err
	}
	q := `show tables`
	rows, err := db.Query(q)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var tableNames []string
	for rows.Next() {
		var tableName string
		if err := rows.Scan(&tableName); err != nil {
			return nil, err
		}
		tableNames = append(tableNames, tableName)
	}
	return tableNames, nil
}

func filterSQLAccessingSystemTables(sqls utils.Set[utils.Query]) (utils.Set[utils.Query], error) {
	s := utils.NewSet[utils.Query]()
	for _, sql := range sqls.ToList() {
		accessSystemTable := false
		tables, err := utils.CollectTableNamesFromSQL(sql.SchemaName, sql.Text)
		if err != nil {
			return nil, err
		}
		for _, t := range tables.ToList() {
			if utils.IsTiDBSystemTableName(t) {
				accessSystemTable = true
				break
			}
		}
		if !accessSystemTable {
			s.Add(sql)
		}
	}
	return s, nil
}
