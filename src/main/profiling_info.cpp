#include "duckdb/main/profiling_info.hpp"

#include "duckdb/common/enum_util.hpp"
#include "duckdb/main/query_profiler.hpp"

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
	case MetricsType::EXTRA_INFO:
		return "\"" + QueryProfiler::JSONSanitize(metrics.extra_info) + "\"";
	case MetricsType::OPERATOR_CARDINALITY:
		return to_string(metrics.operator_cardinality);
	case MetricsType::OPERATOR_TIMING:
		return to_string(metrics.operator_timing);
	}
	return "";
}

void ProfilingInfo::PrintAllMetricsToSS(std::ostream &ss, const string &depth) {
	for (auto &metric : settings) {
		ss << depth << "   \"" << StringUtil::Lower(EnumUtil::ToString(metric)) << "\": " << GetMetricAsString(metric)
		   << ",\n";
	}
}

} // namespace duckdb
