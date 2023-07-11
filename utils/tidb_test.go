package utils

import (
	"bytes"
	"fmt"
	"testing"
	"time"
)

func TestStartTiDB(t *testing.T) {
	cmd, err := StartTiDB("")
	if err != nil {
		panic(err)
	}

	time.Sleep(time.Second * 10)
	stdout := cmd.Stdout.(*bytes.Buffer)
	fmt.Println(stdout.String())

	if err := cmd.Process.Kill(); err != nil {
		panic(err)
	}
}
