#include "duckdb/main/profiling_info.hpp"

#include "duckdb/common/enum_util.hpp"
#include "duckdb/main/query_profiler.hpp"
#include "duckdb/logging/log_manager.hpp"

#include "yyjson.hpp"

using namespace duckdb_yyjson; // NOLINT

namespace duckdb {

ProfilingInfo::ProfilingInfo(const profiler_settings_t &n_settings, const idx_t depth) : settings(n_settings) {
	// Expand.
	if (depth == 0) {
		settings.insert(MetricsType::QUERY_NAME);
	} else {
		settings.insert(MetricsType::OPERATOR_NAME);
		settings.insert(MetricsType::OPERATOR_TYPE);
	}
	for (const auto &metric : settings) {
		Expand(expanded_settings, metric);
	}

	// Reduce.
	if (depth == 0) {
		auto op_metrics = OperatorScopeSettings();
		for (const auto metric : op_metrics) {
			settings.erase(metric);
		}
	} else {
		auto root_metrics = RootScopeSettings();
		for (const auto metric : root_metrics) {
			settings.erase(metric);
		}
	}
	ResetMetrics();
}

profiler_settings_t ProfilingInfo::DefaultSettings() {
	return {MetricsType::ATTACH_LOAD_STORAGE_LATENCY,
	        MetricsType::ATTACH_REPLAY_WAL_LATENCY,
	        MetricsType::BLOCKED_THREAD_TIME,
	        MetricsType::CHECKPOINT_LATENCY,
	        MetricsType::CPU_TIME,
	        MetricsType::CUMULATIVE_CARDINALITY,
	        MetricsType::CUMULATIVE_ROWS_SCANNED,
	        MetricsType::EXTRA_INFO,
	        MetricsType::LATENCY,
	        MetricsType::OPERATOR_CARDINALITY,
	        MetricsType::OPERATOR_NAME,
	        MetricsType::OPERATOR_ROWS_SCANNED,
	        MetricsType::OPERATOR_TIMING,
	        MetricsType::OPERATOR_TYPE,
	        MetricsType::RESULT_SET_SIZE,
	        MetricsType::ROWS_RETURNED,
	        MetricsType::SYSTEM_PEAK_BUFFER_MEMORY,
	        MetricsType::SYSTEM_PEAK_TEMP_DIR_SIZE,
	        MetricsType::TOTAL_BYTES_READ,
	        MetricsType::TOTAL_BYTES_WRITTEN,
	        MetricsType::TOTAL_MEMORY_ALLOCATED,
	        MetricsType::WAITING_TO_ATTACH_LATENCY,
	        MetricsType::WAL_REPLAY_ENTRY_COUNT,
	        MetricsType::COMMIT_LOCAL_STORAGE_LATENCY,
	        MetricsType::WRITE_TO_WAL_LATENCY,
	        MetricsType::QUERY_NAME};
}

profiler_settings_t ProfilingInfo::RootScopeSettings() {
	return {MetricsType::ATTACH_LOAD_STORAGE_LATENCY,
	        MetricsType::ATTACH_REPLAY_WAL_LATENCY,
	        MetricsType::BLOCKED_THREAD_TIME,
	        MetricsType::CHECKPOINT_LATENCY,
	        MetricsType::LATENCY,
	        MetricsType::ROWS_RETURNED,
	        MetricsType::TOTAL_BYTES_READ,
	        MetricsType::TOTAL_BYTES_WRITTEN,
	        MetricsType::WAITING_TO_ATTACH_LATENCY,
	        MetricsType::WAL_REPLAY_ENTRY_COUNT,
	        MetricsType::COMMIT_LOCAL_STORAGE_LATENCY,
	        MetricsType::WRITE_TO_WAL_LATENCY,
	        MetricsType::QUERY_NAME};
}

profiler_settings_t ProfilingInfo::OperatorScopeSettings() {
	return {MetricsType::OPERATOR_CARDINALITY, MetricsType::OPERATOR_ROWS_SCANNED, MetricsType::OPERATOR_TIMING,
	        MetricsType::OPERATOR_NAME, MetricsType::OPERATOR_TYPE};
}

void ProfilingInfo::ResetMetrics() {
	metrics.clear();
	for (auto &metric : expanded_settings) {
		if (MetricsUtils::IsOptimizerMetric(metric) || MetricsUtils::IsPhaseTimingMetric(metric)) {
			metrics[metric] = Value::CreateValue(0.0);
			continue;
		}

		switch (metric) {
		case MetricsType::QUERY_NAME:
			metrics[metric] = Value::CreateValue("");
			break;
		case MetricsType::LATENCY:
		case MetricsType::BLOCKED_THREAD_TIME:
		case MetricsType::CPU_TIME:
		case MetricsType::OPERATOR_TIMING:
		case MetricsType::WAITING_TO_ATTACH_LATENCY:
		case MetricsType::ATTACH_LOAD_STORAGE_LATENCY:
		case MetricsType::ATTACH_REPLAY_WAL_LATENCY:
		case MetricsType::CHECKPOINT_LATENCY:
		case MetricsType::COMMIT_LOCAL_STORAGE_LATENCY:
		case MetricsType::WRITE_TO_WAL_LATENCY:
			metrics[metric] = Value::CreateValue(0.0);
			break;
		case MetricsType::OPERATOR_NAME:
			metrics[metric] = Value::CreateValue("");
			break;
		case MetricsType::OPERATOR_TYPE:
			metrics[metric] = Value::CreateValue<uint8_t>(0);
			break;
		case MetricsType::ROWS_RETURNED:
		case MetricsType::RESULT_SET_SIZE:
		case MetricsType::CUMULATIVE_CARDINALITY:
		case MetricsType::OPERATOR_CARDINALITY:
		case MetricsType::CUMULATIVE_ROWS_SCANNED:
		case MetricsType::OPERATOR_ROWS_SCANNED:
		case MetricsType::SYSTEM_PEAK_BUFFER_MEMORY:
		case MetricsType::SYSTEM_PEAK_TEMP_DIR_SIZE:
		case MetricsType::TOTAL_BYTES_READ:
		case MetricsType::TOTAL_BYTES_WRITTEN:
		case MetricsType::TOTAL_MEMORY_ALLOCATED:
		case MetricsType::WAL_REPLAY_ENTRY_COUNT:
			metrics[metric] = Value::CreateValue<uint64_t>(0);
			break;
		case MetricsType::EXTRA_INFO:
			metrics[metric] = Value::MAP(InsertionOrderPreservingMap<string>());
			break;
		default:
			throw InternalException("MetricsType" + EnumUtil::ToString(metric) + "not implemented");
		}
	}
}

bool ProfilingInfo::Enabled(const profiler_settings_t &settings, const MetricsType metric) {
	if (settings.find(metric) != settings.end()) {
		return true;
	}
	return false;
}

void ProfilingInfo::Expand(profiler_settings_t &settings, const MetricsType metric) {
	settings.insert(metric);

	switch (metric) {
	case MetricsType::CPU_TIME:
		settings.insert(MetricsType::OPERATOR_TIMING);
		return;
	case MetricsType::CUMULATIVE_CARDINALITY:
		settings.insert(MetricsType::OPERATOR_CARDINALITY);
		return;
	case MetricsType::CUMULATIVE_ROWS_SCANNED:
		settings.insert(MetricsType::OPERATOR_ROWS_SCANNED);
		return;
	case MetricsType::CUMULATIVE_OPTIMIZER_TIMING:
	case MetricsType::ALL_OPTIMIZERS: {
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

string ProfilingInfo::GetMetricAsString(const MetricsType metric) const {
	if (!Enabled(settings, metric)) {
		throw InternalException("Metric %s not enabled", EnumUtil::ToString(metric));
	}

	// The metric cannot be NULL and must be initialized.
	D_ASSERT(!metrics.at(metric).IsNull());
	if (metric == MetricsType::OPERATOR_TYPE) {
		const auto type = PhysicalOperatorType(metrics.at(metric).GetValue<uint8_t>());
		return EnumUtil::ToString(type);
	}
	return metrics.at(metric).ToString();
}

void ProfilingInfo::WriteMetricsToLog(ClientContext &context) {
	auto &logger = Logger::Get(context);
	if (logger.ShouldLog(MetricsLogType::NAME, MetricsLogType::LEVEL)) {
		for (auto &metric : settings) {
			logger.WriteLog(MetricsLogType::NAME, MetricsLogType::LEVEL,
			                MetricsLogType::ConstructLogMessage(metric, metrics[metric]));
		}
	}
}

void ProfilingInfo::WriteMetricsToJSON(yyjson_mut_doc *doc, yyjson_mut_val *dest) {
	for (auto &metric : settings) {
		auto metric_str = StringUtil::Lower(EnumUtil::ToString(metric));
		auto key_val = yyjson_mut_strcpy(doc, metric_str.c_str());
		auto key_ptr = yyjson_mut_get_str(key_val);

		if (metric == MetricsType::EXTRA_INFO) {
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

		switch (metric) {
		case MetricsType::QUERY_NAME:
		case MetricsType::OPERATOR_NAME:
			yyjson_mut_obj_add_strcpy(doc, dest, key_ptr, metrics[metric].GetValue<string>().c_str());
			break;
		case MetricsType::LATENCY:
		case MetricsType::BLOCKED_THREAD_TIME:
		case MetricsType::CPU_TIME:
		case MetricsType::OPERATOR_TIMING:
		case MetricsType::WAITING_TO_ATTACH_LATENCY:
		case MetricsType::ATTACH_LOAD_STORAGE_LATENCY:
		case MetricsType::ATTACH_REPLAY_WAL_LATENCY:
		case MetricsType::COMMIT_LOCAL_STORAGE_LATENCY:
		case MetricsType::WRITE_TO_WAL_LATENCY:
		case MetricsType::CHECKPOINT_LATENCY: {
			yyjson_mut_obj_add_real(doc, dest, key_ptr, metrics[metric].GetValue<double>());
			break;
		}
		case MetricsType::OPERATOR_TYPE: {
			yyjson_mut_obj_add_strcpy(doc, dest, key_ptr, GetMetricAsString(metric).c_str());
			break;
		}
		case MetricsType::ROWS_RETURNED:
		case MetricsType::RESULT_SET_SIZE:
		case MetricsType::CUMULATIVE_CARDINALITY:
		case MetricsType::OPERATOR_CARDINALITY:
		case MetricsType::CUMULATIVE_ROWS_SCANNED:
		case MetricsType::OPERATOR_ROWS_SCANNED:
		case MetricsType::SYSTEM_PEAK_BUFFER_MEMORY:
		case MetricsType::SYSTEM_PEAK_TEMP_DIR_SIZE:
		case MetricsType::WAL_REPLAY_ENTRY_COUNT:
		case MetricsType::TOTAL_BYTES_READ:
		case MetricsType::TOTAL_BYTES_WRITTEN:
		case MetricsType::TOTAL_MEMORY_ALLOCATED: {
			yyjson_mut_obj_add_uint(doc, dest, key_ptr, metrics[metric].GetValue<uint64_t>());
			break;
		}
		default:
			throw NotImplementedException("MetricsType %s not implemented", EnumUtil::ToString(metric));
		}
	}
}

} // namespace duckdb
