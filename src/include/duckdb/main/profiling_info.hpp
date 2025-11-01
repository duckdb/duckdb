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

public:
	ProfilingInfo() = default;
	explicit ProfilingInfo(const profiler_settings_t &n_settings, const idx_t depth = 0);
	ProfilingInfo(ProfilingInfo &) = default;
	ProfilingInfo &operator=(ProfilingInfo const &) = default;

public:
	static profiler_settings_t DefaultSettings();
	static profiler_settings_t RootScopeSettings();
	static profiler_settings_t OperatorScopeSettings();

public:
	void ResetMetrics();
	//! Returns true, if the query profiler must collect this metric.
	static bool Enabled(const profiler_settings_t &settings, const MetricsType metric);
	//! Expand metrics depending on the collection of other metrics.
	static void Expand(profiler_settings_t &settings, const MetricsType metric);

public:
	string GetMetricAsString(const MetricsType metric) const;
	void WriteMetricsToLog(ClientContext &context);
	void WriteMetricsToJSON(duckdb_yyjson::yyjson_mut_doc *doc, duckdb_yyjson::yyjson_mut_val *destination);

public:
	template <class METRIC_TYPE>
	METRIC_TYPE GetMetricValue(const MetricsType type) const {
		auto val = metrics.at(type);
		return val.GetValue<METRIC_TYPE>();
	}

	template <class METRIC_TYPE>
	void MetricUpdate(const MetricsType type, const Value &value,
	                  const std::function<METRIC_TYPE(const METRIC_TYPE &, const METRIC_TYPE &)> &update_fun) {
		if (metrics.find(type) == metrics.end()) {
			metrics[type] = value;
			return;
		}
		auto new_value = update_fun(metrics[type].GetValue<METRIC_TYPE>(), value.GetValue<METRIC_TYPE>());
		metrics[type] = Value::CreateValue(new_value);
	}

	template <class METRIC_TYPE>
	void MetricUpdate(const MetricsType type, const METRIC_TYPE &value,
	                  const std::function<METRIC_TYPE(const METRIC_TYPE &, const METRIC_TYPE &)> &update_fun) {
		auto new_value = Value::CreateValue(value);
		MetricUpdate<METRIC_TYPE>(type, new_value, update_fun);
	}

	template <class METRIC_TYPE>
	void MetricSum(const MetricsType type, const Value &value) {
		MetricUpdate<METRIC_TYPE>(type, value, [](const METRIC_TYPE &old_value, const METRIC_TYPE &new_value) {
			return old_value + new_value;
		});
	}

	template <class METRIC_TYPE>
	void MetricSum(const MetricsType type, const METRIC_TYPE &value) {
		auto new_value = Value::CreateValue(value);
		return MetricSum<METRIC_TYPE>(type, new_value);
	}

	template <class METRIC_TYPE>
	void MetricMax(const MetricsType type, const Value &value) {
		MetricUpdate<METRIC_TYPE>(type, value, [](const METRIC_TYPE &old_value, const METRIC_TYPE &new_value) {
			return MaxValue(old_value, new_value);
		});
	}

	template <class METRIC_TYPE>
	void MetricMax(const MetricsType type, const METRIC_TYPE &value) {
		auto new_value = Value::CreateValue(value);
		return MetricMax<METRIC_TYPE>(type, new_value);
	}
};

// Specialization for InsertionOrderPreservingMap<string>
template <>
inline InsertionOrderPreservingMap<string>
ProfilingInfo::GetMetricValue<InsertionOrderPreservingMap<string>>(const MetricsType type) const {
	auto val = metrics.at(type);
	InsertionOrderPreservingMap<string> result;
	auto children = MapValue::GetChildren(val);
	for (auto &child : children) {
		auto struct_children = StructValue::GetChildren(child);
		auto key = struct_children[0].GetValue<string>();
		auto value = struct_children[1].GetValue<string>();
		result.insert(key, value);
	}
	return result;
}
} // namespace duckdb
