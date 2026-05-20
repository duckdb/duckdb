//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/main/metrics_manager.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/common.hpp"
#include "duckdb/common/vector.hpp"

namespace duckdb {

class ClientContext;
class DatabaseInstance;

//! MetricsManager holds descriptors for every metric known to the system:
//! the statically compiled set (from src/common/metrics.json) plus optimizer
//! metrics generated at runtime from the OptimizerType enum.
class MetricsManager {
public:
	MetricsManager() = default;

	DUCKDB_API static MetricsManager &Get(ClientContext &context);
	DUCKDB_API static MetricsManager &Get(DatabaseInstance &db);

	struct MetricInfo {
		string name;
		string metric_type; // "double" | "uint64" | "string" | "map"
		string description;
		string unit;
	};

	//! Returns the total number of known metrics (static + optimizer-generated).
	DUCKDB_API idx_t GetMetricCount() const;

	//! Returns a snapshot of all known metrics.
	DUCKDB_API vector<MetricInfo> GetAllMetrics() const;
};

} // namespace duckdb
