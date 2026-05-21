#include "duckdb/main/metrics_manager.hpp"
#include "duckdb/main/metrics.hpp"
#include "duckdb/main/database.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/common/enums/optimizer_type.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/common/enum_util.hpp"

namespace duckdb {

namespace {

struct MetricDescriptor {
	const char *name;
	const char *metric_type;
	const char *description;
	const char *unit;
};

} // namespace

// clang-format off
#define DUCKDB_METRIC(METRIC) \
	{METRIC::Name, METRIC::TypeStr, METRIC::Description, METRIC::Unit}
#define FINAL_METRIC \
	{nullptr, nullptr, nullptr, nullptr}

static const MetricDescriptor internal_metrics[] = {
	DUCKDB_METRIC(MetricQueryCPUTime),
	DUCKDB_METRIC(MetricQuerySQL),
	DUCKDB_METRIC(MetricQueryTotalIntermediateRows),
	DUCKDB_METRIC(MetricQueryTotalIntermediateSizeBytes),
	DUCKDB_METRIC(MetricQueryTotalRowGroupsScanned),
	DUCKDB_METRIC(MetricQueryTotalRowGroupsToScan),
	DUCKDB_METRIC(MetricQueryTotalRowsScanned),
	DUCKDB_METRIC(MetricQueryTotalTime),
	DUCKDB_METRIC(MetricSystemBlockedThreadTime),
	DUCKDB_METRIC(MetricSystemPeakBufferMemory),
	DUCKDB_METRIC(MetricSystemPeakTempDirSize),
	DUCKDB_METRIC(MetricSystemTotalMemoryAllocated),
	DUCKDB_METRIC(MetricIOTotalBytesRead),
	DUCKDB_METRIC(MetricIOTotalBytesWritten),
	DUCKDB_METRIC(MetricStorageAttachLoadStorageLatency),
	DUCKDB_METRIC(MetricStorageAttachReplayWALLatency),
	DUCKDB_METRIC(MetricStorageCheckpointLatency),
	DUCKDB_METRIC(MetricStorageCommitLocalStorageLatency),
	DUCKDB_METRIC(MetricStorageTotalVacuumTime),
	DUCKDB_METRIC(MetricStorageWaitingToAttachLatency),
	DUCKDB_METRIC(MetricStorageWALReplayEntryCount),
	DUCKDB_METRIC(MetricStorageWriteToWALLatency),
	DUCKDB_METRIC(MetricOperatorCPUTime),
	DUCKDB_METRIC(MetricOperatorExtraInfo),
	DUCKDB_METRIC(MetricOperatorIntermediateRows),
	DUCKDB_METRIC(MetricOperatorIntermediateSizeBytes),
	DUCKDB_METRIC(MetricOperatorRowGroupsScanned),
	DUCKDB_METRIC(MetricOperatorRowsScanned),
	DUCKDB_METRIC(MetricOperatorTiming),
	DUCKDB_METRIC(MetricOperatorTotalRowGroupsToScan),
	DUCKDB_METRIC(MetricOperatorType),
	DUCKDB_METRIC(MetricOptimizerTotalTime),
	DUCKDB_METRIC(MetricParserTotalTime),
	DUCKDB_METRIC(MetricPlannerBindingTime),
	DUCKDB_METRIC(MetricPlannerTotalTime),
	DUCKDB_METRIC(MetricPhysicalPlannerColumnBinding),
	DUCKDB_METRIC(MetricPhysicalPlannerCreatePlan),
	DUCKDB_METRIC(MetricPhysicalPlannerResolveTypes),
	DUCKDB_METRIC(MetricPhysicalPlannerTotalTime),
	FINAL_METRIC,
};
// clang-format on

#undef DUCKDB_METRIC
#undef FINAL_METRIC

MetricsManager &MetricsManager::Get(ClientContext &context) {
	return DatabaseInstance::GetDatabase(context).GetMetricsManager();
}

MetricsManager &MetricsManager::Get(DatabaseInstance &db) {
	return db.GetMetricsManager();
}

idx_t MetricsManager::GetMetricCount() const {
	lock_guard<mutex> guard(registered_metrics_lock);
	idx_t count = sizeof(internal_metrics) / sizeof(MetricDescriptor) - 1;
	count += static_cast<idx_t>(OptimizerType::PARTIAL_AGGREGATE_PUSHDOWN) -
	         static_cast<idx_t>(OptimizerType::EXPRESSION_REWRITER) + 1;
	count += registered_metrics.size();
	return count;
}

vector<MetricInfo> MetricsManager::GetAllMetrics() const {
	lock_guard<mutex> guard(registered_metrics_lock);
	vector<MetricInfo> result;
	for (idx_t i = 0; internal_metrics[i].name; i++) {
		MetricInfo info;
		info.name = internal_metrics[i].name;
		info.metric_type = internal_metrics[i].metric_type;
		info.description = internal_metrics[i].description;
		info.unit = internal_metrics[i].unit;
		result.push_back(std::move(info));
	}
	// Optimizer metrics are generated at runtime from the OptimizerType enum.
	for (auto opt = static_cast<uint32_t>(OptimizerType::EXPRESSION_REWRITER);
	     opt <= static_cast<uint32_t>(OptimizerType::PARTIAL_AGGREGATE_PUSHDOWN); opt++) {
		auto opt_type = static_cast<OptimizerType>(opt);
		string opt_name = StringUtil::Lower(EnumUtil::ToString(opt_type));
		MetricInfo info;
		info.name = "optimizer." + opt_name;
		info.metric_type = "double";
		info.description = "Time spent in the " + opt_name + " optimizer";
		info.unit = "seconds";
		result.push_back(std::move(info));
	}
	// Extension-registered metrics.
	for (const auto &m : registered_metrics) {
		result.push_back(m);
	}
	return result;
}

void MetricsManager::RegisterMetric(MetricInfo info) {
	lock_guard<mutex> guard(registered_metrics_lock);
	for (const auto &existing : registered_metrics) {
		if (existing.name == info.name) {
			return; // silently ignore duplicate
		}
	}
	registered_metrics.push_back(std::move(info));
}

} // namespace duckdb
