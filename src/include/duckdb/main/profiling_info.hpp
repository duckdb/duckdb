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
	//! Enabling a metric adds it to this set.
	profiler_settings_t settings;
	//! This set contains the expanded to-be-collected metrics, which can differ from 'settings'.
	profiler_settings_t expanded_settings;
	//! Contains all enabled metrics.
	profiler_metrics_t metrics;
	//! Additional metrics.
	// FIXME: move to metrics.
	InsertionOrderPreservingMap<string> extra_info;

public:
	ProfilingInfo() = default;
	explicit ProfilingInfo(const profiler_settings_t &n_settings, const idx_t depth = 0);
	ProfilingInfo(ProfilingInfo &) = default;
	ProfilingInfo &operator=(ProfilingInfo const &) = default;

public:
	static profiler_settings_t DefaultSettings();
	static profiler_settings_t DefaultRootSettings();
	static profiler_settings_t DefaultOperatorSettings();

public:
	void ResetMetrics();
	//! Returns true, if the query profiler must collect this metric.
	static bool Enabled(const profiler_settings_t &settings, const MetricsType metric);
	//! Expand metrics depending on the collection of other metrics.
	static void Expand(profiler_settings_t &settings, const MetricsType metric);

public:
	string GetMetricAsString(const MetricsType metric) const;
	void WriteMetricsToJSON(duckdb_yyjson::yyjson_mut_doc *doc, duckdb_yyjson::yyjson_mut_val *destination);

public:
	template <class METRIC_TYPE>
	METRIC_TYPE GetMetricValue(const MetricsType type) const {
		auto val = metrics.at(type);
		return val.GetValue<METRIC_TYPE>();
	}

	template <class METRIC_TYPE>
	void AddToMetric(const MetricsType type, const Value &value) {
		D_ASSERT(!metrics[type].IsNull());
		if (metrics.find(type) == metrics.end()) {
			metrics[type] = value;
			return;
		}
		auto new_value = metrics[type].GetValue<METRIC_TYPE>() + value.GetValue<METRIC_TYPE>();
		metrics[type] = Value::CreateValue(new_value);
	}

	template <class METRIC_TYPE>
	void AddToMetric(const MetricsType type, const METRIC_TYPE &value) {
		auto new_value = Value::CreateValue(value);
		return AddToMetric<METRIC_TYPE>(type, new_value);
	}
};
} // namespace duckdb
