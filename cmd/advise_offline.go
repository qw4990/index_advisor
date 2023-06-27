package cmd

import (
	"fmt"
	"os"
	"path"
	"sort"
	"strings"

	"github.com/qw4990/index_advisor/advisor"
	"github.com/qw4990/index_advisor/optimizer"
	"github.com/qw4990/index_advisor/utils"
	"github.com/spf13/cobra"
)

type adviseOfflineCmdOpt struct {
	maxNumIndexes int
	maxIndexWidth int

	dsn          string
	queryPath    string
	schemaPath   string
	statsPath    string
	output       string
	costModelVer string
	queries      string
	logLevel     string
}

func NewAdviseOfflineCmd() *cobra.Command {
	var opt adviseOfflineCmdOpt
	cmd := &cobra.Command{
		Use:   "advise-offline",
		Short: "advise some indexes for the specified workload",
		Long:  `advise some indexes for the specified workload`,
		RunE: func(cmd *cobra.Command, args []string) error {
			utils.SetLogLevel(opt.logLevel)

			dsnWithoutDB, dbName := utils.GetDBNameFromDSN(opt.dsn)
			if dbName == "" {
				return fmt.Errorf("invalid dsn: %s, no database specified", opt.dsn)
			}
			utils.Infof("connect to %s", opt.dsn)
			db, err := optimizer.NewTiDBWhatIfOptimizer(dsnWithoutDB) // the DB may not exist yet
			if err != nil {
				return err
			}
			skip, err := loadSchemaIntoCluster(db, opt.schemaPath)
			if err != nil {
				return err
			}
			if err := loadStatsIntoCluster(db, opt.statsPath, skip); err != nil {
				return err
			}
			if err := db.Execute(`use ` + dbName); err != nil {
				return err
			}

			queries, err := utils.LoadQueries(dbName, opt.queryPath)
			if err != nil {
				return err
			}
			if opt.queries != "" {
				qs := strings.Split(opt.queries, ",")
				queries = utils.FilterBySQLAlias(queries, qs)
			}

			tableNames, err := utils.CollectTableNamesFromQueries(dbName, queries)
			if err != nil {
				return err
			}
			tableSchemas, err := getTableSchemas(db, tableNames)
			if err != nil {
				return err
			}

			workload := utils.WorkloadInfo{
				Queries:      queries,
				TableSchemas: tableSchemas,
			}

			// set cost-model-version
			if err := db.Execute(fmt.Sprintf("set @@tidb_cost_model_version = %v", opt.costModelVer)); err != nil {
				return nil
			}

			indexes, err := advisor.IndexAdvise(db, workload, advisor.Parameter{
				MaxNumberIndexes: opt.maxNumIndexes,
				MaxIndexWidth:    opt.maxIndexWidth,
			})
			if err != nil {
				return err
			}
			return outputAdviseResult(indexes, workload, db, opt.output)
		},
	}

	cmd.Flags().IntVar(&opt.maxNumIndexes, "max-num-indexes", 5, "max number of indexes to recommend, 0 means no limit")
	cmd.Flags().IntVar(&opt.maxIndexWidth, "max-index-width", 3, "the max number of columns in recommended indexes")

	cmd.Flags().StringVar(&opt.dsn, "dsn", "root:@tcp(127.0.0.1:4000)/test", "dsn")
	cmd.Flags().StringVar(&opt.queryPath, "query-path", "", "(required) query file or dictionary path")
	cmd.Flags().StringVar(&opt.schemaPath, "schema-path", "", "(optional) schema file path")
	cmd.Flags().StringVar(&opt.statsPath, "stats-path", "", "(optional) stats dictionary path")
	cmd.Flags().StringVar(&opt.output, "output", "", "output directory to save the result")
	cmd.Flags().StringVar(&opt.costModelVer, "cost-model-ver", "2", "cost model version, 1 or 2")
	cmd.Flags().StringVar(&opt.queries, "queries", "", "queries to consider, e.g. 'q1, q2'")
	cmd.Flags().StringVar(&opt.logLevel, "log-level", "info", "log level, one of 'debug', 'info', 'warning', 'error'")
	return cmd
}

func outputAdviseResult(indexes utils.Set[utils.Index], workload utils.WorkloadInfo, optimizer optimizer.WhatIfOptimizer, savePath string) error {
	// index DDL statements
	indexList := indexes.ToList()
	sort.Slice(indexList, func(i, j int) bool { // to make the result stable
		return indexList[i].Key() < indexList[j].Key()
	})
	indexDDLStmts := make([]string, 0, len(indexList))
	for _, index := range indexList {
		indexDDLStmts = append(indexDDLStmts, index.DDL())
	}

	// query plan changes
	planChanges, err := getPlanChanges(optimizer, workload, indexList)
	if err != nil {
		return err
	}
	var originalWorkloadCost, optimizerWorkloadCost float64
	for _, change := range planChanges {
		originalWorkloadCost += change.OriPlan.PlanCost()
		optimizerWorkloadCost += change.OptPlan.PlanCost()
	}

	// summary content
	var summaryContent string
	summaryContent += fmt.Sprintf("Total Queries in the workload: %d\n", workload.Queries.Size())
	summaryContent += fmt.Sprintf("Total number of indexes: %d\n", len(indexList))
	for _, ddlStmt := range indexDDLStmts {
		summaryContent += fmt.Sprintf("  %s;\n", ddlStmt)
	}
	if len(indexDDLStmts) == 0 {
		summaryContent += "  (no beneficial index recommended)\n"
	}
	summaryContent += fmt.Sprintf("Total original workload cost: %.2E\n", originalWorkloadCost)
	summaryContent += fmt.Sprintf("Total optimized workload cost: %.2E\n", optimizerWorkloadCost)
	summaryContent += fmt.Sprintf("Total cost reduction ratio: %.2f%%\n", 100*(1-optimizerWorkloadCost/originalWorkloadCost))
	summaryContent += fmt.Sprintf("Top %d queries with the most cost reduction:\n", utils.Min(len(planChanges), 5))
	for i := 0; i < utils.Min(len(planChanges), 5); i++ {
		change := planChanges[i]
		summaryContent += fmt.Sprintf("  Alias: %s, Cost Reduction Ratio: %.2E->%.2E(%.2f)\n", change.SQL.Alias,
			change.OriPlan.PlanCost(), change.OptPlan.PlanCost(), change.OptPlan.PlanCost()/change.OriPlan.PlanCost())
	}

	fmt.Println(summaryContent)
	if savePath != "" {
		os.RemoveAll(savePath) // clear all existing data
		os.MkdirAll(savePath, 0777)

		// summary
		if err := utils.SaveContentTo(path.Join(savePath, "summary.txt"), summaryContent); err != nil {
			return err
		}

		// DDL statements
		ddlContent := strings.Join(indexDDLStmts, ";\n")
		if err := utils.SaveContentTo(path.Join(savePath, "ddl.sql"), ddlContent); err != nil {
			return err
		}

		// plan changes
		for i, change := range planChanges {
			var content string
			content += fmt.Sprintf("Alias: %s\n", change.SQL.Alias)
			content += fmt.Sprintf("Query: \n%s\n\n", change.SQL.Text)
			content += fmt.Sprintf("Original Cost: %.2E\n", change.OriPlan.PlanCost())
			content += fmt.Sprintf("Optimized Cost: %.2E\n", change.OptPlan.PlanCost())
			content += fmt.Sprintf("Cost Reduction Ratio: %.2f\n", change.OptPlan.PlanCost()/change.OriPlan.PlanCost())
			content += "\n\n===================== original plan =====================\n"
			content += change.OriPlan.Format()
			content += "\n\n===================== optimized plan =====================\n"
			content += change.OptPlan.Format()
			var planPath string
			if change.SQL.Alias != "" {
				planPath = path.Join(savePath, fmt.Sprintf("%s.txt", change.SQL.Alias))
			} else {
				planPath = path.Join(savePath, fmt.Sprintf("q%v.txt", i))
			}
			if err := utils.SaveContentTo(planPath, content); err != nil {
				return err
			}
		}
	}
	return nil
}

type planChange struct {
	SQL     utils.Query
	OriPlan utils.Plan
	OptPlan utils.Plan
}

func getPlanChanges(optimizer optimizer.WhatIfOptimizer, workload utils.WorkloadInfo, indexList []utils.Index) ([]planChange, error) {
	sqls := workload.Queries.ToList()
	var oriPlans, optPlans []utils.Plan
	for _, sql := range sqls {
		p, err := optimizer.Explain(sql.Text)
		if err != nil {
			return nil, err
		}
		oriPlans = append(oriPlans, p)
	}
	for _, idx := range indexList {
		if err := optimizer.CreateHypoIndex(idx); err != nil {
			return nil, err
		}
	}
	for _, sql := range sqls {
		p, err := optimizer.Explain(sql.Text)
		if err != nil {
			return nil, err
		}
		optPlans = append(optPlans, p)
	}
	for _, idx := range indexList {
		if err := optimizer.DropHypoIndex(idx); err != nil {
			return nil, err
		}
	}
	var planChanges []planChange
	for i := range sqls {
		planChanges = append(planChanges, planChange{
			SQL:     sqls[i],
			OriPlan: oriPlans[i],
			OptPlan: optPlans[i],
		})
	}
	sort.Slice(planChanges, func(i, j int) bool {
		return planChanges[i].OptPlan.PlanCost()/planChanges[i].OriPlan.PlanCost() < planChanges[j].OptPlan.PlanCost()/planChanges[j].OriPlan.PlanCost()
	})
	return planChanges, nil
}

func getTableSchemas(db optimizer.WhatIfOptimizer, tableNames utils.Set[utils.TableName]) (utils.Set[utils.TableSchema], error) {
	s := utils.NewSet[utils.TableSchema]()
	for _, t := range tableNames.ToList() {
		schema, err := getTableSchema(db, t.SchemaName, t.TableName)
		if err != nil {
			return nil, err
		}
		s.Add(schema)
	}
	return s, nil
}

func getTableSchema(db optimizer.WhatIfOptimizer, schemaName, tableName string) (utils.TableSchema, error) {
	r, err := db.Query(fmt.Sprintf("show create table %v.%v", schemaName, tableName))
	if err != nil {
		return utils.TableSchema{}, err
	}
	defer r.Close()
	if !r.Next() {
		return utils.TableSchema{}, fmt.Errorf("table %v.%v does not exist", schemaName, tableName)
	}
	var tmp, createTableStmt string
	if err := r.Scan(&tmp, &createTableStmt); err != nil {
		return utils.TableSchema{}, err
	}
	return utils.ParseCreateTableStmt(schemaName, createTableStmt)
}
