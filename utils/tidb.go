package utils

import (
	"bytes"
	"fmt"
	"net"
	"os"
	"os/exec"
	"time"
)

// StartTiDB starts a TiDB server with the given version.
func StartTiDB(ver string) (*exec.Cmd, error) {
	if ver == "" {
		ver = "nightly"
	}
	tiupPath, err := exec.LookPath("tiup")
	if err != nil {
		return nil, fmt.Errorf("failed to find tiup cmd: %v, please install tiup first: https://docs.pingcap.com/tidb/dev/tiup-overview", err)
	}

	port, err := GetFreePort()
	if err != nil {
		return nil, fmt.Errorf("failed to get a free port: %v", err)
	}
	tmpDir, err := GetTempDir()
	if err != nil {
		return nil, fmt.Errorf("failed to get a temp dir: %v", err)
	}

	cmd := exec.Command(tiupPath, fmt.Sprintf("tidb:%v", ver), fmt.Sprintf("-P=%v", port), fmt.Sprintf("--store-path=%v", tmpDir))
	var stdErr bytes.Buffer
	cmd.Stderr = &stdErr

	Infof("Starting TiDB %v", cmd.String())
	if err := cmd.Start(); err != nil {
		return nil, fmt.Errorf("failed to start TiDB: %v", err)
	}

	Infof("Wait for TiDB to start, pid: %v", cmd.Process.Pid)
	ok := false
	for i := 0; i < 20; i++ {
		// TODO:
		time.Sleep(time.Second)
		Infof("Wait for TiDB to start, pid: %v", cmd.Process.Pid)
	}
	if !ok {
		cmd.Process.Kill()
		return nil, fmt.Errorf("failed to start TiDB, stderr: %v", stdErr.String())
	}

	return cmd, nil
}

// GetTempDir returns an temporary directory path
func GetTempDir() (string, error) {
	return os.MkdirTemp("", "index_advisor_tidb_tmp")
}

// GetFreePort asks the kernel for a free open port that is ready to use.
func GetFreePort() (int, error) {
	addr, err := net.ResolveTCPAddr("tcp", "localhost:0")
	if err != nil {
		return 0, err
	}

	l, err := net.ListenTCP("tcp", addr)
	if err != nil {
		return 0, err
	}
	defer l.Close()
	return l.Addr().(*net.TCPAddr).Port, nil
}
