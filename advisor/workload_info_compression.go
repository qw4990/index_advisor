package advisor

import (
	"github.com/qw4990/index_advisor/utils"
)

// NoneWorkloadInfoCompress does nothing.
func NoneWorkloadInfoCompress(workloadInfo utils.WorkloadInfo) utils.WorkloadInfo {
	return workloadInfo
}

// DigestWorkloadInfoCompress compresses queries by digest.
func DigestWorkloadInfoCompress(workloadInfo utils.WorkloadInfo) utils.WorkloadInfo {
	compressed := workloadInfo
	compressed.SQLs = compressBySQLDigest(compressed.SQLs)
	return compressed
}

func compressBySQLDigest(sqls utils.Set[utils.SQL]) utils.Set[utils.SQL] {
	s := utils.NewSet[utils.SQL]()
	digestFreq := make(map[string]int)
	digestSQL := make(map[string]utils.SQL)
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
