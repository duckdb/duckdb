//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/main/profiling_info.hpp
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

namespace duckdb_yyjson {
struct yyjson_mut_doc;
struct yyjson_mut_val;
} // namespace duckdb_yyjson

namespace duckdb {

class ProfilingInfo {
public:
	// Enabling a metric adds it to this set.
	profiler_settings_t settings;
	// Contains all enabled metrics.
	profiler_metrics_t metrics;
	// Additional metrics.
	// FIXME: move to metrics.
	InsertionOrderPreservingMap<string> extra_info;

public:
	ProfilingInfo() = default;
	explicit ProfilingInfo(profiler_settings_t &n_settings, idx_t depth = 0) : settings(n_settings) {
		if (depth == 0) {
			settings.insert(MetricsType::QUERY_NAME);
		} else {
			settings.insert(MetricsType::OPERATOR_TYPE);
		}
		ResetMetrics();
	}
	ProfilingInfo(ProfilingInfo &) = default;
	ProfilingInfo &operator=(ProfilingInfo const &) = default;

public:
	static profiler_settings_t DefaultSettings();
	static profiler_settings_t DefaultOperatorSettings();
	static profiler_settings_t AllSettings();

public:
	void ResetMetrics();
	bool Enabled(const MetricsType setting) const;

public:
	string GetMetricAsString(MetricsType setting) const;
	void WriteMetricsToJSON(duckdb_yyjson::yyjson_mut_doc *doc, duckdb_yyjson::yyjson_mut_val *destination);

public:
	template <class METRIC_TYPE>
	METRIC_TYPE GetMetricValue(const MetricsType setting) const {
		auto val = metrics.at(setting);
		return val.GetValue<METRIC_TYPE>();
	}

	template <class METRIC_TYPE>
	void AddToMetric(const MetricsType setting, const Value &value) {
		D_ASSERT(!metrics[setting].IsNull());
		if (metrics.find(setting) == metrics.end()) {
			metrics[setting] = value;
			return;
		}
		auto new_value = metrics[setting].GetValue<METRIC_TYPE>() + value.GetValue<METRIC_TYPE>();
		metrics[setting] = Value::CreateValue(new_value);
	}

	template <class METRIC_TYPE>
	void AddToMetric(const MetricsType setting, const METRIC_TYPE &value) {
		auto new_value = Value::CreateValue(value);
		return AddToMetric<METRIC_TYPE>(setting, new_value);
	}
};
} // namespace duckdb
