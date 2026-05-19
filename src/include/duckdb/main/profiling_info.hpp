//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/main/profiling_info.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/common.hpp"
#include "duckdb/common/enum_util.hpp"
#include "duckdb/common/enums/output_type.hpp"
#include "duckdb/common/enums/profiler_format.hpp"
#include "duckdb/common/progress_bar/progress_bar.hpp"
#include "duckdb/common/types/value.hpp"
#include "duckdb/common/unordered_set.hpp"
#include "duckdb/common/constants.hpp"
#include "duckdb/common/enums/metric_type.hpp"

namespace duckdb_yyjson {
struct yyjson_mut_doc;
struct yyjson_mut_val;
} // namespace duckdb_yyjson

namespace duckdb {

struct QueryProfileResult;
enum class ProfilingParameterNames : uint8_t { FORMAT, COVERAGE, SAVE_LOCATION, MODE, METRICS };

class ProfilingInfo {
public:
	ProfilingInfo() = default;
	explicit ProfilingInfo(const profiler_settings_t &n_settings, const idx_t depth = 0);
	ProfilingInfo(ProfilingInfo &) = default;
	ProfilingInfo &operator=(ProfilingInfo const &) = default;

public:
	void ResetMetrics();
	//! Returns true, if the query profiler must collect this metric.
	bool EnabledForCollection(const MetricType metric) const;
	//! Returns true, if the user requested this metric
	bool Enabled(const MetricType metric) const;
	//! Expand metrics depending on the collection of other metrics.
	static void Expand(profiler_settings_t &settings, const MetricType metric);
	void SetMetricValue(MetricType type, Value new_value);

	const profiler_metrics_t &GetMetrics() const {
		return metrics;
	}
	profiler_metrics_t &GetMetricsMutable() {
		return metrics;
	}

public:
	void WriteMetricsToLog(ClientContext &context) const;
	//! Copy all enabled metrics into a QueryProfileResult node using lowercase string keys
	void MetricsToProfileResult(QueryProfileResult &result) const;

private:
	//! Enabling a metric adds it to this set.
	profiler_settings_t settings;
	//! This set contains the expanded to-be-collected metrics, which can differ from 'settings'.
	profiler_settings_t expanded_settings;
	//! Contains all enabled metrics.
	profiler_metrics_t metrics;
};

} // namespace duckdb
