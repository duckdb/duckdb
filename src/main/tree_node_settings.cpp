#include "duckdb/main/tree_node_settings.hpp"

namespace duckdb {

void TreeNodeSettings::SetSetting(const TreeNodeSettingsType &setting, const Value &value) {
	metrics[setting] = value;
}

Value TreeNodeSettings::GetSetting(const TreeNodeSettingsType &setting) {
	return metrics[setting];
}

Value TreeNodeSettings::GetSetting(const TreeNodeSettingsType &setting) const {
	return metrics.at(setting);
}

void TreeNodeSettings::SetMetrics(unordered_map<TreeNodeSettingsType, Value> &n_metrics) {
	this->metrics = n_metrics;
}

unordered_map<TreeNodeSettingsType, Value> &TreeNodeSettings::GetMetrics() {
	return metrics;
}

void TreeNodeSettings::ResetMetrics() {
	metrics.clear();
	metrics = default_metrics;
}

bool TreeNodeSettings::SettingEnabled(const TreeNodeSettingsType setting) const {
	return metrics.find(setting) != metrics.end();
}

void TreeNodeSettings::AddToExistingSetting(const TreeNodeSettingsType &setting, const Value &value) {
	if (SettingEnabled(setting)) {
		Value existing = GetSetting(setting);
		if (existing.type() == value.type()) {
			SetSetting(setting, Value::);
		}
	}
}

}
