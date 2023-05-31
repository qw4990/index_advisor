package main

import (
	"regexp"
	"strings"
)

const (
	stringRegex          = `([^\\])'((')|([^\\])*?([^\\])')`
	doubleQuoteStringRgx = `([^\\])"((")|([^\\])*?([^\\])")`
	intRegex             = `([^a-zA-Z])-?\d+(\.\d+)?`
	hashRegex            = `('\d+\\.*?')`
)

type Cluster struct {
	SQLs      []SQL
	Frequency int
}
type clusterList []*Cluster
type WorkloadInfoCompressionAlgo func(workloadInfo WorkloadInfo) WorkloadInfo

// NoneWorkloadInfoCompress does nothing.
func NoneWorkloadInfoCompress(workloadInfo WorkloadInfo) WorkloadInfo {
	return workloadInfo
}

// NaiveWorkloadInfoCompress does nothing.
func NaiveWorkloadInfoCompress(workloadInfo WorkloadInfo) WorkloadInfo {

	return workloadInfo
}

func ClusteringWorkloadInfoCompress(workloadInfo WorkloadInfo) WorkloadInfo {
	clusters := make(clusterList, 0)

	for _, sql := range workloadInfo.SQLs.ToList() {
		clusters.addSQLToCluster(sql)
	}

	newSQLs := NewSet[SQL]()
	for _, c := range clusters {
		maxFreq := 0
		maxSQLIndex := -1

		for i, sql := range c.SQLs {
			if sql.Frequency > maxFreq {
				maxFreq = sql.Frequency
				maxSQLIndex = i
			}
		}

		if maxSQLIndex >= 0 {
			maxSQL := c.SQLs[maxSQLIndex]
			maxSQL.Frequency = c.Frequency
			newSQLs.Add(maxSQL)
		}
	}

	workloadInfo.SQLs = newSQLs
	return workloadInfo
}

func getTemplate(query string) string {

	stringReg := regexp.MustCompile(stringRegex)
	doubleQuoteReg := regexp.MustCompile(doubleQuoteStringRgx)
	intReg := regexp.MustCompile(intRegex)
	hashReg := regexp.MustCompile(hashRegex)

	template := hashReg.ReplaceAllString(query, "@@@")
	template = stringReg.ReplaceAllString(template, "${1}&&&")
	template = doubleQuoteReg.ReplaceAllString(template, "${1}&&&")
	template = intReg.ReplaceAllString(template, "${1}#")

	return strings.TrimSpace(template)
}

// template + tpye + schemaName same
func (cl *clusterList) addSQLToCluster(sql SQL) {
	for _, c := range *cl {
		if c.SQLs[0].Type() == sql.Type() && c.SQLs[0].SchemaName == sql.SchemaName && getTemplate(c.SQLs[0].Text) == getTemplate(sql.Text) {
			c.SQLs = append(c.SQLs, sql)
			c.Frequency += sql.Frequency
			return
		}
	}
	*cl = append(*cl, &Cluster{
		SQLs:      []SQL{sql},
		Frequency: sql.Frequency,
	})
}
