package cmd

import (
	"strings"
	"testing"

	"github.com/qw4990/index_advisor/optimizer"
	"github.com/qw4990/index_advisor/utils"
)

func TestPreCheck(t *testing.T) {
	server, err := utils.StartLocalTiDBServer("v7.1.0")
	must(err)
	db, err := optimizer.NewTiDBWhatIfOptimizer(server.DSN())
	must(err)
	err = PreCheck(db)
	if !strings.Contains(err.Error(), "your TiDB version does not support hypothetical index feature") {
		panic("should not pass")
	}
	must(server.Release())

	server, err = utils.StartLocalTiDBServer("nightly")
	must(err)
	db, err = optimizer.NewTiDBWhatIfOptimizer(server.DSN())
	must(err)
	err = PreCheck(db)
	must(err)
	must(server.Release())
}

func TestOnlineModeSimple(t *testing.T) {
	server, err := utils.StartLocalTiDBServer("nightly")
	must(err)
	defer server.Release()
	db, err := optimizer.NewTiDBWhatIfOptimizer(server.DSN())
	must(err)

	must(db.Execute(`use test`))
	must(db.Execute(`create table t (a int, b int, c int)`))
	must(db.Execute(`select a from t where a=1`))
	must(db.Execute(`select a, b from t where a=1 and b=1`))

	_, _, _, err = adviseOnlineMode(adviseOnlineCmdOpt{
		maxNumIndexes:           5,
		maxIndexWidth:           3,
		dsn:                     server.DSN(),
		output:                  "",
		logLevel:                "info",
		querySchemas:            []string{},
		queryExecTimeThreshold:  0,
		queryExecCountThreshold: 0,
		queryPath:               "",
	})
	mustTrue(strings.Contains(err.Error(), "query-schemas is not specified"), err.Error())

	_, _, _, err = adviseOnlineMode(adviseOnlineCmdOpt{
		maxNumIndexes:           5,
		maxIndexWidth:           3,
		dsn:                     server.DSN(),
		output:                  "",
		logLevel:                "info",
		querySchemas:            []string{"mysql"},
		queryExecTimeThreshold:  0,
		queryExecCountThreshold: 0,
		queryPath:               "",
	})
	mustTrue(strings.Contains(err.Error(), "no queries are found"), err.Error())

	result, _, _, err := adviseOnlineMode(adviseOnlineCmdOpt{
		maxNumIndexes:           1,
		maxIndexWidth:           3,
		dsn:                     server.DSN(),
		output:                  "",
		logLevel:                "info",
		querySchemas:            []string{"test"},
		queryExecTimeThreshold:  0,
		queryExecCountThreshold: 0,
		queryPath:               "",
	})
	must(err)
	mustTrue(result.Size() == 1)
	mustTrue(result.ToList()[0].DDL() == "CREATE INDEX idx_a_b ON test.t (a, b)")
}
