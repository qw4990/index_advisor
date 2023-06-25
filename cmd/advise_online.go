package cmd

import (
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

	dsn     string
	schemas []string
}

func NewAdviseOnlineCmd() *cobra.Command {
	var opt adviseOnlineCmdOpt
	cmd := &cobra.Command{
		Use:   "advise-online",
		Short: "advise some indexes for the specified workload",
		Long:  `advise some indexes for the specified workload`,
		RunE: func(cmd *cobra.Command, args []string) error {
			db, err := optimizer.NewTiDBWhatIfOptimizer(opt.dsn)
			if err != nil {
				return err
			}

			sqls, err := readQueriesFromStatementSummary(db, opt.schemas)
			if err != nil {
				return err
			}
			sqls, err = filterSQLAccessingSystemTables(sqls)
			if err != nil {
				return err
			}
			tables, err := readTableSchemas(db, opt.schemas)
			if err != nil {
				return err
			}
			info := utils.WorkloadInfo{
				SQLs:         sqls,
				TableSchemas: tables,
			}

			indexes, err := advisor.IndexAdvise(db, info, advisor.Parameter{
				MaxNumberIndexes: opt.maxNumIndexes,
				MaxIndexWidth:    opt.maxIndexWidth,
			})
			if err != nil {
				return err
			}
			return outputAdviseResult(indexes, info, db, "")
		},
	}

	cmd.Flags().IntVar(&opt.maxNumIndexes, "max-num-indexes", 10, "max number of indexes to recommend, 0 means no limit")
	cmd.Flags().IntVar(&opt.maxNumIndexes, "max-index-width", 3, "the max number of columns in recommended indexes")

	cmd.Flags().StringVar(&opt.dsn, "dsn", "root:@tcp(127.0.0.1:4000)/test", "dsn")
	cmd.Flags().StringSliceVar(&opt.schemas, "schemas", []string{}, "the schema(database) name to consider, e.g. 'test1, test2'")
	return cmd
}

func readQueriesFromStatementSummary(db optimizer.WhatIfOptimizer, schemas []string) (utils.Set[utils.SQL], error) {
	s := utils.NewSet[utils.SQL]()
	for _, table := range []string{
		`information_schema.statements_summary`,
		`information_schema.statements_summary_history`,
	} {
		// TODO: consider Execute statements
		q := fmt.Sprintf(`select SCHEMA_NAME, DIGEST, QUERY_SAMPLE_TEXT, EXEC_COUNT, AVG_LATENCY from %v `+
			`where SCHEMA_NAME in ('%s') and stmt_type='Select'`, table, strings.Join(schemas, "', '"))
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
			s.Add(utils.SQL{
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

func filterSQLAccessingSystemTables(sqls utils.Set[utils.SQL]) (utils.Set[utils.SQL], error) {
	s := utils.NewSet[utils.SQL]()
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
