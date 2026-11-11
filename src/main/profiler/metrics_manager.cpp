#include "duckdb/main/profiler/metrics_manager.hpp"
#include "duckdb/common/algorithm.hpp"
#include "duckdb/main/profiler/metrics.hpp"
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
	DUCKDB_METRIC(MetricQueryTotalBytesScanned),
	DUCKDB_METRIC(MetricQueryTotalIntermediateRows),
	DUCKDB_METRIC(MetricQueryTotalIntermediateSizeBytes),
	DUCKDB_METRIC(MetricQueryTotalRowGroupsScanned),
	DUCKDB_METRIC(MetricQueryTotalRowGroupsToScan),
	DUCKDB_METRIC(MetricQueryTotalRowsScanned),
	DUCKDB_METRIC(MetricQueryTotalTime),
	DUCKDB_METRIC(MetricSystemBlockedThreadTime),
	DUCKDB_METRIC(MetricSystemPeakBufferMemory),
	DUCKDB_METRIC(MetricSystemPeakStreamingBufferSize),
	DUCKDB_METRIC(MetricSystemPeakTempDirSize),
	DUCKDB_METRIC(MetricSystemTotalBytesSpilled),
	DUCKDB_METRIC(MetricSystemTotalMemoryAllocated),
	DUCKDB_METRIC(MetricIOTotalBytesRead),
	DUCKDB_METRIC(MetricIOTotalBytesWritten),
	DUCKDB_METRIC(MetricIOTotalReadOperations),
	DUCKDB_METRIC(MetricIOTotalReadTime),
	DUCKDB_METRIC(MetricIOTotalWriteOperations),
	DUCKDB_METRIC(MetricIOTotalWriteTime),
	DUCKDB_METRIC(MetricStorageAttachLoadStorageLatency),
	DUCKDB_METRIC(MetricStorageAttachReplayWALLatency),
	DUCKDB_METRIC(MetricStorageCheckpointLatency),
	DUCKDB_METRIC(MetricStorageCommitLocalStorageLatency),
	DUCKDB_METRIC(MetricStorageTotalVacuumTime),
	DUCKDB_METRIC(MetricStorageWaitingToAttachLatency),
	DUCKDB_METRIC(MetricStorageWALReplayEntryCount),
	DUCKDB_METRIC(MetricStorageWriteToWALLatency),
	DUCKDB_METRIC(MetricOperatorBytesScanned),
	DUCKDB_METRIC(MetricOperatorCPUTime),
	DUCKDB_METRIC(MetricOperatorExtraInfo),
	DUCKDB_METRIC(MetricOperatorIntermediateRows),
	DUCKDB_METRIC(MetricOperatorIntermediateSizeBytes),
	DUCKDB_METRIC(MetricOperatorName),
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

struct MetricsManager::InternalMetrics {
	InternalMetrics() {
		for (idx_t i = 0; internal_metrics[i].name; i++) {
			MetricInfo info;
			info.name = internal_metrics[i].name;
			info.metric_type = internal_metrics[i].metric_type;
			info.description = internal_metrics[i].description;
			info.unit = internal_metrics[i].unit;
			metrics.push_back(std::move(info));
		}
		D_ASSERT(metrics.size() == GeneratedMetricInfo::MaxMetricIndex);
		// optimizer metrics are generated at runtime from the list of optimizers
		for (auto &optimizer : ListAllOptimizers()) {
			auto type = OptimizerTypeFromString(optimizer);
			auto type_value = static_cast<idx_t>(type);
			if (optimizer_indexes.size() <= type_value) {
				optimizer_indexes.resize(type_value + 1);
			}
			optimizer_indexes[type_value] = metrics.size();
			string opt_name = StringUtil::Lower(EnumUtil::ToString(type));
			MetricInfo info;
			info.name = "optimizer." + opt_name;
			info.metric_type = "double";
			info.description = "Time spent in the " + opt_name + " optimizer";
			info.unit = "seconds";
			metrics.push_back(std::move(info));
		}
		for (idx_t i = 0; i < metrics.size(); i++) {
			indexes[metrics[i].name] = i;
		}
	}

	vector<MetricInfo> metrics;
	unordered_map<string, idx_t> indexes;
	//! By the value of the OptimizerType
	vector<optional_idx> optimizer_indexes;
};

const MetricsManager::InternalMetrics &MetricsManager::GetInternalMetrics() {
	static const InternalMetrics internal;
	return internal;
}

idx_t MetricsManager::InternalMetricCount() {
	return GetInternalMetrics().metrics.size();
}

const MetricInfo &MetricsManager::GetInternalMetric(idx_t metric_index) {
	return GetInternalMetrics().metrics[metric_index];
}

idx_t MetricsManager::OptimizerMetricIndex(OptimizerType type) {
	auto &optimizer_indexes = GetInternalMetrics().optimizer_indexes;
	auto type_value = static_cast<idx_t>(type);
	if (type_value >= optimizer_indexes.size() || !optimizer_indexes[type_value].IsValid()) {
		throw InternalException("Optimizer %s has no metric: add it to the list of optimizers",
		                        EnumUtil::ToString(type));
	}
	return optimizer_indexes[type_value].GetIndex();
}

optional_idx MetricsManager::TryGetInternalMetricIndex(const string &name) {
	auto &indexes = GetInternalMetrics().indexes;
	auto entry = indexes.find(name);
	return entry == indexes.end() ? optional_idx() : optional_idx(entry->second);
}

//! The metrics extensions registered in any database of the process, by their index after the internal ones
struct MetricsManager::RegisteredMetrics {
	mutex lock;
	vector<MetricInfo> metrics;
	unordered_map<string, idx_t> indexes;
	//! The number of known indexes, internal ones included, read without the lock
	atomic<idx_t> known_count {InternalMetricCount()};
};

MetricsManager::RegisteredMetrics &MetricsManager::GetRegisteredMetrics() {
	static RegisteredMetrics registered;
	return registered;
}

idx_t MetricsManager::KnownMetricCount() {
	return GetRegisteredMetrics().known_count.load(std::memory_order_acquire);
}

MetricInfo MetricsManager::GetMetric(idx_t metric_index) {
	if (metric_index < InternalMetricCount()) {
		return GetInternalMetric(metric_index);
	}
	auto &registered = GetRegisteredMetrics();
	lock_guard<mutex> guard(registered.lock);
	D_ASSERT(metric_index - InternalMetricCount() < registered.metrics.size());
	return registered.metrics[metric_index - InternalMetricCount()];
}

optional_idx MetricsManager::TryGetMetricIndex(const string &name) {
	auto index = TryGetInternalMetricIndex(name);
	if (index.IsValid()) {
		return index;
	}
	auto &registered = GetRegisteredMetrics();
	lock_guard<mutex> guard(registered.lock);
	auto entry = registered.indexes.find(name);
	return entry == registered.indexes.end() ? optional_idx() : optional_idx(entry->second);
}

idx_t MetricsManager::GetMetricCount() const {
	lock_guard<mutex> guard(registered_metrics_lock);
	return InternalMetricCount() + registered_here.size();
}

vector<MetricInfo> MetricsManager::GetAllMetrics() const {
	vector<MetricInfo> result = GetInternalMetrics().metrics;
	lock_guard<mutex> guard(registered_metrics_lock);
	// extension-registered metrics
	for (auto index : registered_here) {
		result.push_back(GetMetric(index));
	}
	return result;
}

idx_t MetricsManager::RegisterMetric(MetricInfo info) {
	auto internal_index = TryGetInternalMetricIndex(info.name);
	if (internal_index.IsValid()) {
		auto &existing = GetInternalMetric(internal_index.GetIndex());
		if (existing.metric_type != info.metric_type) {
			throw InvalidInputException("Metric \"%s\" is one of the profiler's own, of type \"%s\"", info.name,
			                            existing.metric_type);
		}
		return internal_index.GetIndex();
	}
	idx_t index;
	{
		auto &registered = GetRegisteredMetrics();
		lock_guard<mutex> guard(registered.lock);
		auto entry = registered.indexes.find(info.name);
		if (entry != registered.indexes.end()) {
			index = entry->second;
			auto &existing = registered.metrics[index - InternalMetricCount()];
			if (existing.metric_type != info.metric_type) {
				throw InvalidInputException("Metric \"%s\" is already registered with type \"%s\"", info.name,
				                            existing.metric_type);
			}
		} else {
			index = InternalMetricCount() + registered.metrics.size();
			if (index >= MAX_METRIC_COUNT) {
				throw InvalidInputException("Cannot register metric \"%s\": a process can know at most %llu metrics",
				                            info.name, MAX_METRIC_COUNT);
			}
			registered.indexes[info.name] = index;
			registered.metrics.push_back(std::move(info));
			registered.known_count.store(index + 1, std::memory_order_release);
		}
	}
	lock_guard<mutex> guard(registered_metrics_lock);
	if (std::find(registered_here.begin(), registered_here.end(), index) == registered_here.end()) {
		registered_here.push_back(index);
	}
	return index;
}

bool MetricsManager::IsRegisteredMetric(const string &name) const {
	auto index = TryGetMetricIndex(name);
	if (!index.IsValid() || index.GetIndex() < InternalMetricCount()) {
		return false;
	}
	lock_guard<mutex> guard(registered_metrics_lock);
	return std::find(registered_here.begin(), registered_here.end(), index.GetIndex()) != registered_here.end();
}

} // namespace duckdb
