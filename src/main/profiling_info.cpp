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
	    MetricsType::CUMULATIVE_CARDINALITY,
	    MetricsType::OPERATOR_CARDINALITY,
	    MetricsType::OPERATOR_TIMING,
	};
}

void ProfilingInfo::ResetSettings() {
	settings.clear();
	settings = DefaultSettings();
}

void ProfilingInfo::ResetMetrics() {
	metrics = {};
}

bool ProfilingInfo::Enabled(const MetricsType setting) const {
	if (settings.find(setting) != settings.end()) {
		return true;
	}
	if (setting == MetricsType::OPERATOR_TIMING && Enabled(MetricsType::CPU_TIME)) {
		return true;
	}
	if (setting == MetricsType::OPERATOR_CARDINALITY && Enabled(MetricsType::CUMULATIVE_CARDINALITY)) {
		return true;
	}
	return false;
}

string ProfilingInfo::GetMetricAsString(MetricsType setting) {
	if (!Enabled(setting)) {
		throw InternalException("Metric %s not enabled", EnumUtil::ToString(setting));
	}

	if (setting == MetricsType::EXTRA_INFO) {
		string result;
		for (auto &it : extra_info) {
			if (!result.empty()) {
				result += ", ";
			}
			result += StringUtil::Format("%s: %s", it.first, it.second);
		}
		return "\"" + result + "\"";
	}

	if (metrics[setting].IsNull()) {
		return "0";
	}
	return metrics[setting].ToString();
}

void ProfilingInfo::WriteMetricsToJSON(yyjson_mut_doc *doc, yyjson_mut_val *dest) {
	for (auto &metric : settings) {
		auto metric_str = StringUtil::Lower(EnumUtil::ToString(metric));
		auto key_val = yyjson_mut_strcpy(doc, metric_str.c_str());
		auto key_ptr = yyjson_get_str(reinterpret_cast<yyjson_val *>(key_val));

		if (metric == MetricsType::EXTRA_INFO) {
			auto extra_info_obj = yyjson_mut_obj(doc);

			for (auto &it : extra_info) {
				auto &key = it.first;
				auto &value = it.second;
				auto splits = StringUtil::Split(value, "\n");
				if (splits.size() > 1) {
					auto list_items = yyjson_mut_arr(doc);
					for (auto &split : splits) {
						yyjson_mut_arr_add_strcpy(doc, list_items, split.c_str());
					}
					yyjson_mut_obj_add_val(doc, extra_info_obj, key.c_str(), list_items);
				} else {
					yyjson_mut_obj_add_strcpy(doc, extra_info_obj, key.c_str(), value.c_str());
				}
			}
			yyjson_mut_obj_add_val(doc, dest, key_ptr, extra_info_obj);
		}

		if (metrics[metric].IsNull()) {
			yyjson_mut_obj_add_str(doc, dest, key_ptr, "0");
			continue;
		}

		switch (metric) {
		case MetricsType::CPU_TIME:
		case MetricsType::OPERATOR_TIMING: {
			yyjson_mut_obj_add_real(doc, dest, key_ptr, metrics[metric].GetValue<double>());
			break;
		}
        case MetricsType::CUMULATIVE_CARDINALITY:
		case MetricsType::OPERATOR_CARDINALITY: {
			yyjson_mut_obj_add_uint(doc, dest, key_ptr, metrics[metric].GetValue<uint64_t>());
			break;
		}
		default:
			throw NotImplementedException("MetricsType %s not implemented", EnumUtil::ToString(metric));
		}
	}
}

} // namespace duckdb
