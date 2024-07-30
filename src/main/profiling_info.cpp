#include "duckdb/main/profiling_info.hpp"

#include "duckdb/common/enum_util.hpp"
#include "duckdb/main/query_profiler.hpp"

#include "yyjson.hpp"

using namespace duckdb_yyjson; // NOLINT

namespace duckdb {

void ProfilingInfo::SetSettings(profiler_settings_t const &n_settings) {
	this->settings = n_settings;
}

const profiler_settings_t &ProfilingInfo::GetSettings() {
	return settings;
}

profiler_settings_t ProfilingInfo::DefaultSettings() {
	return {
	    MetricsType::CPU_TIME,
	    MetricsType::EXTRA_INFO,
	    MetricsType::OPERATOR_CARDINALITY,
	    MetricsType::OPERATOR_TIMING,
	};
}

void ProfilingInfo::ResetSettings() {
	settings.clear();
	settings = DefaultSettings();
}

void ProfilingInfo::ResetMetrics() {
	metrics = Metrics();
}

bool ProfilingInfo::Enabled(const MetricsType setting) const {
	if (settings.find(setting) != settings.end()) {
		return true;
	}
	if (setting == MetricsType::OPERATOR_TIMING && Enabled(MetricsType::CPU_TIME)) {
		return true;
	}
	return false;
}

string ProfilingInfo::GetMetricAsString(MetricsType setting) const {
	switch (setting) {
	case MetricsType::CPU_TIME:
		return to_string(metrics.cpu_time);
	case MetricsType::EXTRA_INFO: {
		string result;
		for (auto &it : QueryProfiler::JSONSanitize(metrics.extra_info)) {
			if (!result.empty()) {
				result += ", ";
			}
			result += StringUtil::Format("%s: %s", it.first, it.second);
		}
		return "\"" + result + "\"";
	}
	case MetricsType::OPERATOR_CARDINALITY:
		return to_string(metrics.operator_cardinality);
	case MetricsType::OPERATOR_TIMING:
		return to_string(metrics.operator_timing);
	default:
		throw NotImplementedException("MetricsType %s not implemented", EnumUtil::ToString(setting));
	}
}

void ProfilingInfo::WriteMetricsToJSON(yyjson_mut_doc *doc, yyjson_mut_val *dest) {
	for (auto &metric : settings) {
		switch (metric) {
		case MetricsType::CPU_TIME:
			yyjson_mut_obj_add_real(doc, dest, "cpu_time", metrics.cpu_time);
			break;
		case MetricsType::EXTRA_INFO: {
			auto extra_info = yyjson_mut_obj(doc);
			for (auto &it : metrics.extra_info) {
				auto &key = it.first;
				auto &value = it.second;
				auto splits = StringUtil::Split(value, "\n");
				if (splits.size() > 1) {
					auto list_items = yyjson_mut_arr(doc);
					for (auto &split : splits) {
						yyjson_mut_arr_add_strcpy(doc, list_items, split.c_str());
					}
					yyjson_mut_obj_add_val(doc, extra_info, key.c_str(), list_items);
				} else {
					yyjson_mut_obj_add_strcpy(doc, extra_info, key.c_str(), value.c_str());
				}
			}
			yyjson_mut_obj_add_val(doc, dest, "extra_info", extra_info);
			break;
		}
		case MetricsType::OPERATOR_CARDINALITY: {
			yyjson_mut_obj_add_uint(doc, dest, "operator_cardinality", metrics.operator_cardinality);
			break;
		}
		case MetricsType::OPERATOR_TIMING: {
			yyjson_mut_obj_add_real(doc, dest, "operator_timing", metrics.operator_timing);
			break;
		}
		default:
			throw NotImplementedException("MetricsType %s not implemented", EnumUtil::ToString(metric));
		}
	}
}

} // namespace duckdb
