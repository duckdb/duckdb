#include "duckdb/main/profiler/profiling_utils.hpp"
#include "duckdb/common/string_util.hpp"

namespace duckdb {

void QueryMetrics::FinalizeMetrics(GatheredMetrics &info) {
	info.SetMetric<MetricQuerySQL>(query_sql);
	for (const auto &[key, ns] : string_timings) {
		info.SetMetric(key, static_cast<double>(ns) / 1e9);
	}
	for (const auto &[key, count] : string_counters) {
		info.SetMetric(key, count);
	}
	for (auto &entry : GetLiveMetrics()) {
		info.SetMetric(entry.first, std::move(entry.second));
	}
	info.SetMetric<MetricSystemBlockedThreadTime>(blocked_thread_time);
	info.SetMetric<MetricSystemPeakBufferMemory>(system_peak_buffer_memory);
	info.SetMetric<MetricSystemPeakStreamingBufferSize>(system_peak_streaming_buffer_size);
	info.SetMetric<MetricSystemPeakTempDirSize>(system_peak_temp_dir_size);
}

profiler_metrics_t QueryMetrics::GetLiveMetrics() const {
	profiler_metrics_t result;
	result[MetricIOTotalBytesRead::Name] = Value::UBIGINT(GetBytesRead());
	result[MetricIOTotalBytesWritten::Name] = Value::UBIGINT(GetBytesWritten());
	result[MetricSystemTotalMemoryAllocated::Name] = Value::UBIGINT(GetTotalMemoryAllocated());
	return result;
}

QueryMetrics::QueryMetrics() : bytes_read(0), bytes_written(0), total_memory_allocated(0) {
	Reset();
}

QueryMetrics::~QueryMetrics() = default;

} // namespace duckdb
