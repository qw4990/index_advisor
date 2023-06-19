package advisor

import (
	"github.com/qw4990/index_advisor/utils"
	wk "github.com/qw4990/index_advisor/workload"
)

// NoneWorkloadInfoCompress does nothing.
func NoneWorkloadInfoCompress(workloadInfo wk.WorkloadInfo) wk.WorkloadInfo {
	return workloadInfo
}

// DigestWorkloadInfoCompress compresses queries by digest.
func DigestWorkloadInfoCompress(workloadInfo wk.WorkloadInfo) wk.WorkloadInfo {
	compressed := workloadInfo
	compressed.SQLs = compressBySQLDigest(compressed.SQLs)
	return compressed
}

func compressBySQLDigest(sqls utils.Set[wk.SQL]) utils.Set[wk.SQL] {
	s := utils.NewSet[wk.SQL]()
	digestFreq := make(map[string]int)
	digestSQL := make(map[string]wk.SQL)
	for _, sql := range sqls.ToList() {
		_, digest := utils.NormalizeDigest(sql.Text)
		if _, ok := digestFreq[digest]; ok {
			digestFreq[digest] += sql.Frequency
			existingSQL := digestSQL[digest]
			existingSQL.Frequency = digestFreq[digest]
			s.Add(existingSQL)
		} else {
			digestFreq[digest] = sql.Frequency
			digestSQL[digest] = sql
			s.Add(sql)
		}
	}
	return s
}
