#include "duckdb/main/tree_node_settings.hpp"

namespace duckdb {


void TreeNodeSettings::SetMetrics(unordered_set<TreeNodeSettingsType> &n_metrics) {
	this->metrics = n_metrics;
}

unordered_set<TreeNodeSettingsType> &TreeNodeSettings::GetMetrics() {
	return metrics;
}

void TreeNodeSettings::SetCpuTime(double cpu_time) {
	if (!SettingEnabled(TreeNodeSettingsType::CPU_TIME)) {
		return;
	}
	values.cpu_time = cpu_time;
}

void TreeNodeSettings::AddToCpuTime(double cpu_time) {
	if (!SettingEnabled(TreeNodeSettingsType::CPU_TIME)) {
		return;
	}
	values.cpu_time += cpu_time;
}

double TreeNodeSettings::GetCpuTime() const {
	if (!SettingEnabled(TreeNodeSettingsType::CPU_TIME)) {
		return 0;
	}
	return values.cpu_time;
}

void TreeNodeSettings::SetExtraInfo(string extra_info) {
	if (!SettingEnabled(TreeNodeSettingsType::EXTRA_INFO)) {
		return;
	}
	values.extra_info = extra_info;
}

void TreeNodeSettings::AddToExtraInfo(string extra_info) {
	if (!SettingEnabled(TreeNodeSettingsType::EXTRA_INFO)) {
		return;
	}
	values.extra_info += extra_info;
}

string TreeNodeSettings::GetExtraInfo() const {
	if (!SettingEnabled(TreeNodeSettingsType::EXTRA_INFO)) {
		return "";
	}
	return values.extra_info;
}

void TreeNodeSettings::SetOperatorCardinality(idx_t operator_cardinality) {
	if (!SettingEnabled(TreeNodeSettingsType::OPERATOR_CARDINALITY)) {
		return;
	}
	values.operator_cardinality = operator_cardinality;
}

void TreeNodeSettings::AddToOperatorCardinality(idx_t operator_cardinality) {
	if (!SettingEnabled(TreeNodeSettingsType::OPERATOR_CARDINALITY)) {
		return;
	}
	values.operator_cardinality += operator_cardinality;
}

idx_t TreeNodeSettings::GetOperatorCardinality() const {
	if (!SettingEnabled(TreeNodeSettingsType::OPERATOR_CARDINALITY)) {
		return 0;
	}
	return values.operator_cardinality;
}

void TreeNodeSettings::SetOperatorTiming(double operator_timing) {
	if (!SettingEnabled(TreeNodeSettingsType::OPERATOR_TIMING)) {
		return;
	}
	values.operator_timing = operator_timing;
}

void TreeNodeSettings::AddToOperatorTiming(double operator_timing) {
	if (!SettingEnabled(TreeNodeSettingsType::OPERATOR_TIMING)) {
		return;
	}
	values.operator_timing += operator_timing;
}

double TreeNodeSettings::GetOperatorTiming() const {
	if (!SettingEnabled(TreeNodeSettingsType::OPERATOR_TIMING)) {
		return 0;
	}
	return values.operator_timing;
}


void TreeNodeSettings::ResetMetrics() {
	metrics.clear();
	metrics = default_metrics;
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
			return to_string(GetCpuTime());
		case TreeNodeSettingsType::EXTRA_INFO:
			return JSONSanitize(GetExtraInfo());
		case TreeNodeSettingsType::OPERATOR_CARDINALITY:
			return to_string(GetOperatorCardinality());
		case TreeNodeSettingsType::OPERATOR_TIMING:
			return to_string(GetOperatorTiming());
	}
}

void TreeNodeSettings::PrintAllMetricsToSS(std::ostream &ss, string depth) {
	for (auto &metric : metrics) {
		ss << depth << "   \"" << StringUtil::Lower(EnumUtil::ToString(metric)) << "\": " << "\"" << GetMetricAsString(metric) << "\",\n";
	}
}

}
