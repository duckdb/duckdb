#include "duckdb/main/profiler/metrics_manager.hpp"
#ifdef __MVS__
#include <zos-tls.h>
#endif
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
	idx_t index;
};

} // namespace

// clang-format off
#define DUCKDB_METRIC(METRIC) \
	{METRIC::Name, METRIC::TypeStr, METRIC::Description, METRIC::Unit, METRIC::MetricIndex}
#define FINAL_METRIC \
	{nullptr, nullptr, nullptr, nullptr, 0}

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
			// the generated MetricIndex of every metric is its position in the list
			D_ASSERT(internal_metrics[i].index == i);
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
	// never destroyed, so that a database destroyed at exit can still read it
	static const auto &internal = *new InternalMetrics();
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
		// its timer records nothing
		D_ASSERT(false);
		return NumericLimits<idx_t>::Maximum();
	}
	return optimizer_indexes[type_value].GetIndex();
}

optional_idx MetricsManager::TryGetInternalMetricIndex(const string &name) {
	auto &indexes = GetInternalMetrics().indexes;
	auto entry = indexes.find(name);
	return entry == indexes.end() ? optional_idx() : optional_idx(entry->second);
}

static MetricSlotKind KindOfType(const string &metric_type) {
	if (metric_type == "uint64") {
		return MetricSlotKind::COUNTER;
	}
	if (metric_type == "double") {
		return MetricSlotKind::TIMING;
	}
	return MetricSlotKind::NONE;
}

static atomic<idx_t> next_manager_id {1};

MetricsManager::MetricsManager() : manager_id(next_manager_id++) {
}

idx_t MetricsManager::GetMetricCount() const {
	return InternalMetricCount() + registered_count.load(std::memory_order_acquire);
}

vector<MetricInfo> MetricsManager::GetAllMetrics() const {
	vector<MetricInfo> result = GetInternalMetrics().metrics;
	lock_guard<mutex> guard(registered_metrics_lock);
	// extension-registered metrics
	for (const auto &m : registered_metrics) {
		result.push_back(m);
	}
	return result;
}

MetricInfo MetricsManager::GetMetric(idx_t metric_index) const {
	if (metric_index < InternalMetricCount()) {
		return GetInternalMetric(metric_index);
	}
	lock_guard<mutex> guard(registered_metrics_lock);
	if (metric_index - InternalMetricCount() >= registered_metrics.size()) {
		throw InvalidInputException("Metric index %llu is not the index of a metric of this database", metric_index);
	}
	return registered_metrics[metric_index - InternalMetricCount()];
}

vector<MetricSlotKind> MetricsManager::GetInternalMetricKinds(idx_t begin, idx_t end) {
	vector<MetricSlotKind> kinds;
	for (idx_t i = begin; i < MinValue<idx_t>(end, InternalMetricCount()); i++) {
		kinds.push_back(KindOfType(GetInternalMetric(i).metric_type));
	}
	return kinds;
}

vector<MetricSlotKind> MetricsManager::GetMetricKinds(idx_t begin, idx_t end) const {
	auto kinds = GetInternalMetricKinds(begin, end);
	if (end > InternalMetricCount()) {
		lock_guard<mutex> guard(registered_metrics_lock);
		for (idx_t i = MaxValue<idx_t>(begin, InternalMetricCount()); i < end; i++) {
			kinds.push_back(KindOfType(registered_metrics[i - InternalMetricCount()].metric_type));
		}
	}
	return kinds;
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
	lock_guard<mutex> guard(registered_metrics_lock);
	auto entry = registered_indexes.find(info.name);
	if (entry != registered_indexes.end()) {
		auto &existing = registered_metrics[entry->second - InternalMetricCount()];
		if (existing.metric_type != info.metric_type) {
			throw InvalidInputException("Metric \"%s\" is already registered with type \"%s\"", info.name,
			                            existing.metric_type);
		}
		return entry->second;
	}
	auto index = InternalMetricCount() + registered_metrics.size();
	registered_indexes[info.name] = index;
	registered_metrics.push_back(std::move(info));
	registered_count.store(registered_metrics.size(), std::memory_order_release);
	version.fetch_add(1, std::memory_order_release);
	return index;
}

#ifndef __MINGW32__
const CachedMetricIndexes &MetricsManager::GetCachedIndexes() const {
// a thread's copy of the registered indexes, refreshed after a metric was registered or for another database
#ifdef __MVS__
	static __tlssim<CachedMetricIndexes> current_cache_impl;
#define current_cache (*current_cache_impl.access())
#else
	thread_local CachedMetricIndexes current_cache;
#endif
	auto current_version = version.load(std::memory_order_acquire);
	if (current_cache.manager_id != manager_id || current_cache.version != current_version) {
		lock_guard<mutex> guard(registered_metrics_lock);
		current_cache.manager_id = manager_id;
		current_cache.version = version.load(std::memory_order_relaxed);
		current_cache.indexes = registered_indexes;
	}
	return current_cache;
#ifdef __MVS__
#undef current_cache
#endif
}
#endif

optional_idx MetricsManager::TryGetMetricIndex(const string &name) const {
	// the names extensions update are registered ones, so those are looked up first
	if (registered_count.load(std::memory_order_acquire) > 0) {
#ifndef __MINGW32__
		auto &indexes = GetCachedIndexes().indexes;
		auto entry = indexes.find(name);
		if (entry != indexes.end()) {
			return entry->second;
		}
#else
		lock_guard<mutex> guard(registered_metrics_lock);
		auto entry = registered_indexes.find(name);
		if (entry != registered_indexes.end()) {
			return entry->second;
		}
#endif
	}
	// the profiler's own metrics never change, so they need no cache
	return TryGetInternalMetricIndex(name);
}

bool MetricsManager::IsRegisteredMetric(const string &name) const {
	auto index = TryGetMetricIndex(name);
	return index.IsValid() && index.GetIndex() >= InternalMetricCount();
}

} // namespace duckdb
