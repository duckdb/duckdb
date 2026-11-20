//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/main/profiler/metrics_manager.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/atomic.hpp"
#include "duckdb/common/common.hpp"
#include "duckdb/common/unordered_map.hpp"
#include "duckdb/common/optional_ptr.hpp"
#include "duckdb/common/enums/optimizer_type.hpp"
#include "duckdb/common/mutex.hpp"
#include "duckdb/common/optional_idx.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/main/profiler/metric_info.hpp"

namespace duckdb {

class ClientContext;
class DatabaseInstance;

class MetricsManager;

#ifndef __MINGW32__
//! A thread's copy of the indexes of the metrics a database registered, so that a lookup by name takes no lock
struct CachedMetricIndexes {
	idx_t manager_id = 0;
	idx_t version = 0;
	unordered_map<string, idx_t> indexes;
};
#endif

//! MetricsManager holds descriptors for every metric known to the system:
//! the statically compiled set (from src/common/metrics.json), optimizer
//! metrics generated at runtime from the OptimizerType enum, and any
//! additional metrics registered by extensions at runtime.
class MetricsManager {
public:
	MetricsManager();

	DUCKDB_API static MetricsManager &Get(ClientContext &context);
	DUCKDB_API static MetricsManager &Get(DatabaseInstance &db);

	//! The number of known metrics, which have the indexes 0 to GetMetricCount() - 1; read without a lock
	DUCKDB_API idx_t GetMetricCount() const;

	//! Returns a snapshot of all known metrics, in the order of their indexes.
	DUCKDB_API vector<MetricInfo> GetAllMetrics() const;

	//! Register a metric and return its index in this database; a known name returns its index, a new type throws
	DUCKDB_API idx_t RegisterMetric(MetricInfo info);

	//! Whether an extension registered the metric `name`. The profiler's own metrics cannot be registered.
	DUCKDB_API bool IsRegisteredMetric(const string &name) const;
	//! The index of the metric `name`, or an invalid index; reads a thread-local cache, so it takes no lock
	DUCKDB_API optional_idx TryGetMetricIndex(const string &name) const;
	//! The metric with a known index
	DUCKDB_API MetricInfo GetMetric(idx_t metric_index) const;
	//! How the metric indexes from begin to end, which must be known, are updated
	DUCKDB_API vector<MetricSlotKind> GetMetricKinds(idx_t begin, idx_t end) const;

	//! The number of the profiler's own metrics and optimizer timings, which come before the registered metrics
	DUCKDB_API static idx_t InternalMetricCount();
	//! One of the profiler's own metrics or optimizer timings, by its index
	DUCKDB_API static const MetricInfo &GetInternalMetric(idx_t metric_index);
	//! How the profiler's own metrics and optimizer timings from begin to end are updated
	DUCKDB_API static vector<MetricSlotKind> GetInternalMetricKinds(idx_t begin, idx_t end);
	//! The index of an optimizer's timing, or an index no metric has when the optimizer is not listed
	DUCKDB_API static idx_t OptimizerMetricIndex(OptimizerType type);
	//! The index of one of the profiler's own metrics or optimizer timings, or an invalid index
	DUCKDB_API static optional_idx TryGetInternalMetricIndex(const string &name);

private:
	struct InternalMetrics;
	static const InternalMetrics &GetInternalMetrics();
#ifndef __MINGW32__
	const CachedMetricIndexes &GetCachedIndexes() const;
#endif

	mutable mutex registered_metrics_lock;
	vector<MetricInfo> registered_metrics;
	unordered_map<string, idx_t> registered_indexes;
	//! The number of registered metrics, which changes when the version does
	atomic<idx_t> registered_count {0};
	atomic<idx_t> version {0};
	//! Unique in the process, so that a cache of a destroyed manager is never taken for a new one at its address
	idx_t manager_id;
};

} // namespace duckdb
