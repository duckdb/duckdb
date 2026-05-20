//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/main/gathered_metrics.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/common.hpp"
#include "duckdb/common/enums/output_type.hpp"
#include "duckdb/common/enums/profiler_format.hpp"
#include "duckdb/common/progress_bar/progress_bar.hpp"
#include "duckdb/common/types/value.hpp"
#include "duckdb/common/unordered_set.hpp"
#include "duckdb/common/constants.hpp"
#include "duckdb/common/enums/metric_type.hpp"
#include "duckdb/main/metrics.hpp"

namespace duckdb_yyjson {
struct yyjson_mut_doc;
struct yyjson_mut_val;
} // namespace duckdb_yyjson

namespace duckdb {

struct QueryProfileResult;
enum class ProfilingParameterNames : uint8_t { FORMAT, COVERAGE, SAVE_LOCATION, MODE, METRICS };

class GatheredMetrics {
public:
	GatheredMetrics() = default;
	explicit GatheredMetrics(const profiler_settings_t &n_settings);
	GatheredMetrics(GatheredMetrics &) = default;
	GatheredMetrics &operator=(GatheredMetrics const &) = default;

public:
	void ResetMetrics();
	//! Returns true if this metric is enabled (and should therefore be collected and output).
	bool MetricIsEnabled(const string &key) const;
	void SetMetric(const string &key, Value new_value);
	void SetMetric(const string &key, idx_t value);
	void SetMetric(const string &key, double value);
	void SetMetric(const string &key, const string &value);

	template <class T>
	bool MetricIsEnabled() const {
		return MetricIsEnabled(T::Name);
	}

	template <class T>
	void SetMetric(const typename T::METRIC_TYPE &value) {
		SetMetric(T::Name, value);
	}

	const profiler_metrics_t &GetMetrics() const {
		return metrics;
	}

public:
	void WriteMetricsToLog(ClientContext &context) const;
	//! Copy all enabled metrics into a QueryProfileResult node using lowercase string keys
	void MetricsToProfileResult(QueryProfileResult &result) const;

private:
	//! Enabling a metric adds it to this set (controls both collection and output).
	profiler_settings_t settings;
	//! Contains all enabled metrics.
	profiler_metrics_t metrics;
};

} // namespace duckdb
