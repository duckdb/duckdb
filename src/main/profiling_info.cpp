#include "duckdb/main/profiling_info.hpp"

#include "duckdb/common/enum_util.hpp"
#include "duckdb/main/profiling_utils.hpp"

#include "yyjson.hpp"

using namespace duckdb_yyjson; // NOLINT

namespace duckdb {

ProfilingInfo::ProfilingInfo(const profiler_settings_t &n_settings, const idx_t depth) : settings(n_settings) {
	// Expand.
	if (depth == 0) {
		settings.insert(MetricType::QUERY_NAME);
	} else {
		settings.insert(MetricType::OPERATOR_NAME);
		settings.insert(MetricType::OPERATOR_TYPE);
	}
	for (const auto &metric : settings) {
		Expand(expanded_settings, metric);
	}

	// Reduce.
	if (depth == 0) {
		auto op_metrics = MetricsUtils::GetOperatorMetrics();
		for (const auto metric : op_metrics) {
			settings.erase(metric);
		}
	} else {
		auto root_metrics = MetricsUtils::GetRootScopeMetrics();
		for (const auto metric : root_metrics) {
			settings.erase(metric);
		}
	}
	ResetMetrics();
}

void ProfilingInfo::ResetMetrics() {
	metrics.clear();
	for (auto &metric : expanded_settings) {
		if (MetricsUtils::IsOptimizerMetric(metric) || MetricsUtils::IsPhaseTimingMetric(metric)) {
			metrics[metric] = Value::CreateValue(0.0);
			continue;
		}

		ProfilingUtils::SetMetricToDefault(metrics, metric);
	}
}

bool ProfilingInfo::Enabled(const profiler_settings_t &settings, const MetricType metric) {
	if (settings.find(metric) != settings.end()) {
		return true;
	}
	return false;
}

void ProfilingInfo::Expand(profiler_settings_t &settings, const MetricType metric) {
	settings.insert(metric);

	switch (metric) {
	case MetricType::CPU_TIME:
		settings.insert(MetricType::OPERATOR_TIMING);
		return;
	case MetricType::CUMULATIVE_CARDINALITY:
		settings.insert(MetricType::OPERATOR_CARDINALITY);
		return;
	case MetricType::CUMULATIVE_ROWS_SCANNED:
		settings.insert(MetricType::OPERATOR_ROWS_SCANNED);
		return;
	case MetricType::CUMULATIVE_OPTIMIZER_TIMING:
	case MetricType::ALL_OPTIMIZERS: {
		auto optimizer_metrics = MetricsUtils::GetOptimizerMetrics();
		for (const auto optimizer_metric : optimizer_metrics) {
			settings.insert(optimizer_metric);
		}
		return;
	}
	default:
		return;
	}
}

string ProfilingInfo::GetMetricAsString(const MetricType metric) const {
	if (!Enabled(settings, metric)) {
		throw InternalException("Metric %s not enabled", EnumUtil::ToString(metric));
	}

	// The metric cannot be NULL and must be initialized.
	D_ASSERT(!metrics.at(metric).IsNull());
	if (metric == MetricType::OPERATOR_TYPE) {
		const auto type = PhysicalOperatorType(metrics.at(metric).GetValue<uint8_t>());
		return EnumUtil::ToString(type);
	}
	return metrics.at(metric).ToString();
}

void ProfilingInfo::WriteMetricsToJSON(yyjson_mut_doc *doc, yyjson_mut_val *dest) {
	for (auto &metric : settings) {
		auto metric_str = StringUtil::Lower(EnumUtil::ToString(metric));
		auto key_val = yyjson_mut_strcpy(doc, metric_str.c_str());
		auto key_ptr = yyjson_mut_get_str(key_val);

		if (metric == MetricType::EXTRA_INFO) {
			auto extra_info_obj = yyjson_mut_obj(doc);

			auto extra_info = metrics.at(metric);
			auto children = MapValue::GetChildren(extra_info);
			for (auto &child : children) {
				auto struct_children = StructValue::GetChildren(child);
				auto key = struct_children[0].GetValue<string>();
				auto value = struct_children[1].GetValue<string>();

				auto key_mut = unsafe_yyjson_mut_strncpy(doc, key.c_str(), key.size());
				auto value_mut = unsafe_yyjson_mut_strncpy(doc, value.c_str(), value.size());

				auto splits = StringUtil::Split(value_mut, "\n");
				if (splits.size() > 1) {
					auto list_items = yyjson_mut_arr(doc);
					for (auto &split : splits) {
						yyjson_mut_arr_add_strcpy(doc, list_items, split.c_str());
					}
					yyjson_mut_obj_add_val(doc, extra_info_obj, key_mut, list_items);
				} else {
					yyjson_mut_obj_add_strcpy(doc, extra_info_obj, key_mut, value_mut);
				}
			}
			yyjson_mut_obj_add_val(doc, dest, key_ptr, extra_info_obj);
			continue;
		}

		// The metric cannot be NULL, and should have been 0 initialized.
		D_ASSERT(!metrics[metric].IsNull());

		if (MetricsUtils::IsOptimizerMetric(metric) || MetricsUtils::IsPhaseTimingMetric(metric)) {
			yyjson_mut_obj_add_real(doc, dest, key_ptr, metrics[metric].GetValue<double>());
			continue;
		}

		ProfilingUtils::MetricToJson(doc, dest, key_ptr, metrics, metric);
	}
}

} // namespace duckdb
