#include "duckdb/main/tree_node_settings.hpp"

namespace duckdb {

void TreeNodeSettings::SetMetrics(unordered_set<TreeNodeSettingsType> &n_metrics) {
	this->metrics = n_metrics;
}

unordered_set<TreeNodeSettingsType> &TreeNodeSettings::GetMetrics() {
	return metrics;
}

void TreeNodeSettings::ResetMetrics() {
	metrics.clear();
	metrics = default_metrics;
}

void TreeNodeSettings::ResetValues() {
	values.cpu_time = 0;
	values.extra_info = "";
	values.operator_cardinality = 0;
	values.operator_timing = 0;
}

bool TreeNodeSettings::SettingEnabled(const TreeNodeSettingsType setting) const {
	if (setting == TreeNodeSettingsType::OPERATOR_TIMING && SettingEnabled(TreeNodeSettingsType::CPU_TIME)) {
		return true;
	}
	return metrics.find(setting) != metrics.end();
}

string TreeNodeSettings::GetMetricAsString(TreeNodeSettingsType setting) const {
	switch (setting) {
	case TreeNodeSettingsType::CPU_TIME:
		return to_string(values.cpu_time);
	case TreeNodeSettingsType::EXTRA_INFO:
		return JSONSanitize(values.extra_info);
	case TreeNodeSettingsType::OPERATOR_CARDINALITY:
		return to_string(values.operator_cardinality);
	case TreeNodeSettingsType::OPERATOR_TIMING:
		return to_string(values.operator_timing);
	}
	return "";
}

void TreeNodeSettings::PrintAllMetricsToSS(std::ostream &ss, string depth) {
	for (auto &metric : metrics) {
		ss << depth << "   \"" << StringUtil::Lower(EnumUtil::ToString(metric)) << "\": "
		   << "\"" << GetMetricAsString(metric) << "\",\n";
	}
}

} // namespace duckdb
