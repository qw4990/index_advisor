package cmd

import (
	"bytes"
	"fmt"
	"path"
	"strings"

	"github.com/qw4990/index_advisor/optimizer"
	"github.com/qw4990/index_advisor/utils"
	"github.com/spf13/cobra"
)

type workloadExportCmdOpt struct {
	dsn        string
	statusAddr string
	output     string
	logLevel   string
}

func NewWorkloadExportCmd() *cobra.Command {
	var opt workloadExportCmdOpt
	cmd := &cobra.Command{
		Use:   "workload-export",
		Short: "export workload information (queries, table schema, table statistics) from your TiDB cluster, use `index_advisor workload-export --help` to see more details`",
		Long: `export workload information (queries, table schema, table statistics) from your TiDB cluster.
How it work:
1. connect to your TiDB cluster through the DSN
2. read all queries from the 'STATEMENT_SUMMARY' system table
3. read all table schema from the 'INFORMATION_SCHEMA' database
4. read all statistics from the 'mysql.stats_xxx' system tables
5. store all data into the specified output directory
`,
		RunE: func(cmd *cobra.Command, args []string) error {
			utils.SetLogLevel(opt.logLevel)
			fmt.Printf("[workload-export] start exporting workload information from TiDB cluster %v to %v\n", opt.dsn, opt.output)
			err := exportWorkload(opt)
			if err == nil {
				fmt.Printf("[workload-export] export workload information successfully into %s\n", opt.output)
			} else {
				fmt.Printf("[workload-export] export workload information failed: %v\n", err)
			}
			return err
		},
	}

	cmd.Flags().StringVar(&opt.dsn, "dsn", "root:@tcp(127.0.0.1:4000)/test", "dsn")
	cmd.Flags().StringVar(&opt.statusAddr, "status_address", "http://127.0.0.1:10080", "status address used to download table statistics")
	cmd.Flags().StringVar(&opt.output, "output", "", "output directory to save the result")
	cmd.Flags().StringVar(&opt.logLevel, "log-level", "info", "log level, one of 'debug', 'info', 'warning', 'error'")
	return cmd
}

func exportWorkload(opt workloadExportCmdOpt) error {
	fmt.Printf("[workload-export] prepare dir %v\n", opt.output)
	if err := utils.PrepareDir(opt.output); err != nil {
		return err
	}
	fmt.Printf("[workload-export] connect to %v\n", opt.dsn)
	db, err := optimizer.NewTiDBWhatIfOptimizer(opt.dsn)
	if err != nil {
		return err
	}
	queries, err := readQueriesFromStatementSummary(db, nil, 0, 0)
	if err != nil {
		return err
	}
	queries, err = filterSQLAccessingSystemTables(queries)
	if err != nil {
		return err
	}
	fmt.Printf("[workload-export] read %v queries\n", queries.Size())
	if err := saveQueries(opt, queries); err != nil {
		return err
	}

	tableNames, err := utils.CollectTableNamesFromQueries(queries)
	if err != nil {
		return err
	}
	tables, err := getTableSchemas(db, tableNames)
	if err != nil {
		return err
	}
	if err := saveTableSchemas(opt, tables); err != nil {
		return err
	}

	fmt.Printf("[workload-export] start dumping table statistics for %v tables\n", tableNames.Size())
	statsDir := path.Join(opt.output, "stats")
	fmt.Printf("[workload-export] prepare stats dir %v\n", statsDir)
	if err := utils.PrepareDir(statsDir); err != nil {
		return err
	}
	for _, t := range tableNames.ToList() {
		stats, err := fetchTableStats(opt, t)
		if err != nil {
			return err
		}
		fpath := path.Join(statsDir, fmt.Sprintf("%s_%s.json", t.SchemaName, t.TableName))
		if err := utils.SaveContentTo(fpath, string(stats)); err != nil {
			return err
		}
		fmt.Printf("[workload-export] save table statistics for %v to %v\n", t.Key(), fpath)
	}
	return nil
}

func fetchTableStats(opt workloadExportCmdOpt, table utils.TableName) ([]byte, error) {
	// http://${tidb-server-ip}:${tidb-server-status-port}/stats/dump/${db_name}/${table_name}
	url := fmt.Sprintf("%s/stats/dump/%s/%s", opt.statusAddr, table.SchemaName, table.TableName)
	stats, err := utils.ReadURL(url)
	if err != nil {
		fmt.Printf("[workload-export] fail to dump statistics for %v from %v, err: %v\n", table.Key(), url, err)
		return nil, err
	}
	fmt.Printf("[workload-export] succeed to dump statistics for %v from %v\n", table.Key(), url)
	return stats, err
}

func saveQueries(opt workloadExportCmdOpt, queries utils.Set[utils.Query]) error {
	var buf bytes.Buffer
	for _, q := range queries.ToList() {
		buf.WriteString(fmt.Sprintf("use %s;\n", q.SchemaName))
		text := strings.TrimSpace(q.Text)
		buf.WriteString(text)
		if !strings.HasSuffix(text, ";") {
			buf.WriteString(";")
		}
		buf.WriteString("\n\n")
	}
	fpath := path.Join(opt.output, "queries.sql")
	fmt.Printf("[workload-export] save queries to %v\n", fpath)
	return utils.SaveContentTo(fpath, buf.String())
}

func saveTableSchemas(opt workloadExportCmdOpt, tables utils.Set[utils.TableSchema]) error {
	var buf bytes.Buffer
	for _, t := range tables.ToList() {
		buf.WriteString(fmt.Sprintf("create database if not exists %s;\n", t.SchemaName))
		buf.WriteString(fmt.Sprintf("use %s;\n", t.SchemaName))
		text := strings.TrimSpace(t.CreateStmtText)
		buf.WriteString(text)
		if !strings.HasSuffix(text, ";") {
			buf.WriteString(";")
		}
		buf.WriteString("\n\n")
	}
	fpath := path.Join(opt.output, "schemas.sql")
	fmt.Printf("[workload-export] save table schema into %s\n", fpath)
	return utils.SaveContentTo(fpath, buf.String())
}
