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
