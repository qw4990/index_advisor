package main

type WorkloadInfoCompressionAlgo func(workloadInfo WorkloadInfo) WorkloadInfo

// NoneWorkloadInfoCompress does nothing.
func NoneWorkloadInfoCompress(workloadInfo WorkloadInfo) WorkloadInfo {
	return workloadInfo
}
