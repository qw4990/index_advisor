package cmd

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/qw4990/index_advisor/advisor"
	"github.com/qw4990/index_advisor/optimizer"
	"github.com/qw4990/index_advisor/utils"
	"github.com/qw4990/index_advisor/workload"
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

			sqls := readQueriesFromStatementSummary(db, opt.schemas)
			tables := readTableSchemas(db, opt.schemas)
			info := workload.WorkloadInfo{
				SQLs:         sqls,
				TableSchemas: tables,
			}

			indexes, err := advisor.IndexAdvise(db, info, advisor.Parameter{
				MaxNumberIndexes: opt.maxNumIndexes,
				MaxIndexWidth:    opt.maxIndexWidth,
			})
			utils.Must(err)
			PrintAndSaveAdviseResult("TODO", indexes, info, db)
			return nil
		},
	}

	cmd.Flags().IntVar(&opt.maxNumIndexes, "max-num-indexes", 10, "max number of indexes to recommend, 0 means no limit")
	cmd.Flags().IntVar(&opt.maxNumIndexes, "max-index-width", 3, "the max number of columns in recommended indexes")

	cmd.Flags().StringVar(&opt.dsn, "dsn", "root:@tcp(127.0.0.1:4000)/test", "dsn")
	cmd.Flags().StringSliceVar(&opt.schemas, "schemas", []string{}, "the schema(database) name to consider, e.g. 'test1, test2'")
	return cmd
}

func readQueriesFromStatementSummary(db optimizer.WhatIfOptimizer, schemas []string) utils.Set[workload.SQL] {
	s := utils.NewSet[workload.SQL]()
	for _, table := range []string{
		`information_schema.statements_summary`,
		`information_schema.statements_summary_history`,
	} {
		q := fmt.Sprintf(`select SCHEMA_NAME, DIGEST, DIGEST_TEXT, EXEC_COUNT, AVG_LATENCY from %v `+
			`where SCHEMA_NAME in ('%s')`, table, strings.Join(schemas, "', '"))
		rows, err := db.Query(q)
		utils.Must(err)
		for rows.Next() {
			var schemaName, digest, text, execCountStr, avgLatStr string
			utils.Must(rows.Scan(&schemaName, &digest, &text, &execCountStr, &avgLatStr))
			execCount, err := strconv.Atoi(execCountStr)
			utils.Must(err)
			s.Add(workload.SQL{
				Alias:      digest,
				SchemaName: schemaName,
				Text:       text,
				Frequency:  execCount,
			})
		}
		utils.Must(rows.Close())
	}
	return s
}

func readTableSchemas(db optimizer.WhatIfOptimizer, schemas []string) utils.Set[workload.TableSchema] {
	s := utils.NewSet[workload.TableSchema]()
	for _, schemaName := range schemas {
		tableNames := readTableNames(db, schemaName)
		for _, tableName := range tableNames {
			q := fmt.Sprintf(`show create table %s.%s`, schemaName, tableName)
			rows, err := db.Query(q)
			utils.Must(err)
			for rows.Next() {
				var name, createTableStmt string
				utils.Must(rows.Scan(&name, &createTableStmt))
				tableSchema, err := workload.ParseCreateTableStmt(schemaName, createTableStmt)
				utils.Must(err)
				s.Add(tableSchema)
			}
			rows.Close()
		}
	}
	return s
}

func readTableNames(db optimizer.WhatIfOptimizer, schemaName string) []string {
	utils.Must(db.Execute(fmt.Sprintf(`use %s`, schemaName)))
	q := `show tables`
	rows, err := db.Query(q)
	utils.Must(err)
	defer rows.Close()
	var tableNames []string
	for rows.Next() {
		var tableName string
		utils.Must(rows.Scan(&tableName))
		tableNames = append(tableNames, tableName)
	}
	return tableNames
}
