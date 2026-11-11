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
#include "duckdb/common/enums/optimizer_type.hpp"
#include "duckdb/common/mutex.hpp"
#include "duckdb/common/optional_idx.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/main/profiler/metric_info.hpp"

namespace duckdb {

class ClientContext;
class DatabaseInstance;

//! MetricsManager holds descriptors for every metric known to a database: the statically compiled set (from
//! src/common/metrics.json), optimizer metrics generated at runtime from the OptimizerType enum, and any additional
//! metrics registered by extensions at runtime. A metric's index is the same in every database of the process.
class MetricsManager {
public:
	//! The most metrics a process can know, with the profiler's own
	static constexpr idx_t MAX_METRIC_COUNT = 16384;

	MetricsManager() = default;

	DUCKDB_API static MetricsManager &Get(ClientContext &context);
	DUCKDB_API static MetricsManager &Get(DatabaseInstance &db);

	//! Returns the number of metrics known to this database (static + optimizer-generated + registered).
	DUCKDB_API idx_t GetMetricCount() const;

	//! Returns a snapshot of all metrics known to this database.
	DUCKDB_API vector<MetricInfo> GetAllMetrics() const;

	//! Register an extension metric and return its index, which is the same in every database; throws on a type clash
	DUCKDB_API idx_t RegisterMetric(MetricInfo info);

	//! Whether an extension registered the metric `name` in this database.
	DUCKDB_API bool IsRegisteredMetric(const string &name) const;

	//! The number of metric indexes the process knows, read without a lock
	DUCKDB_API static idx_t KnownMetricCount();
	//! The metric with the index `metric_index`, which must be known
	DUCKDB_API static MetricInfo GetMetric(idx_t metric_index);
	//! The index of the metric `name`, or an invalid index when the process does not know it
	DUCKDB_API static optional_idx TryGetMetricIndex(const string &name);

	//! The number of the profiler's own metrics and optimizer timings, which come before the registered metrics
	DUCKDB_API static idx_t InternalMetricCount();
	//! One of the profiler's own metrics or optimizer timings, by its index
	DUCKDB_API static const MetricInfo &GetInternalMetric(idx_t metric_index);
	//! The index of an optimizer's timing
	DUCKDB_API static idx_t OptimizerMetricIndex(OptimizerType type);
	//! The index of one of the profiler's own metrics or optimizer timings, or an invalid index
	DUCKDB_API static optional_idx TryGetInternalMetricIndex(const string &name);

private:
	struct InternalMetrics;
	struct RegisteredMetrics;
	static const InternalMetrics &GetInternalMetrics();
	static RegisteredMetrics &GetRegisteredMetrics();

	mutable mutex registered_metrics_lock;
	//! The indexes of the metrics registered in this database, in the order they were registered
	vector<idx_t> registered_here;
};

} // namespace duckdb
