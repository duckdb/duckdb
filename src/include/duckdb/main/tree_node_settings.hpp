//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/main/tree_node_settings.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/case_insensitive_map.hpp"
#include "duckdb/common/common.hpp"
#include "duckdb/common/enums/output_type.hpp"
#include "duckdb/common/enums/profiler_format.hpp"
#include "duckdb/common/progress_bar/progress_bar.hpp"
#include "duckdb/common/types/value.hpp"

namespace duckdb {

enum class TreeNodeSettingsType : uint8_t { CPU_TIME, EXTRA_INFO, OPERATOR_CARDINALITY, OPERATOR_TIMING };

const unordered_map<TreeNodeSettingsType, Value> default_metrics = {
    {TreeNodeSettingsType::CPU_TIME, Value()},
    {TreeNodeSettingsType::EXTRA_INFO, Value()},
    {TreeNodeSettingsType::OPERATOR_CARDINALITY, Value()},
    {TreeNodeSettingsType::OPERATOR_TIMING, Value()},
};

class TreeNodeSettings {
private:
	// map of metrics with their values; only enabled metrics are present in the map
	unordered_map<TreeNodeSettingsType, Value> metrics = default_metrics;

public:
	TreeNodeSettings() = default;
	TreeNodeSettings(TreeNodeSettings &) = default;
	TreeNodeSettings &operator=(TreeNodeSettings &) = default;


public:
	//set the value of a metric
	void SetSetting(const TreeNodeSettingsType &setting, const Value &value);
	//add to the value of a metric
	void AddToExistingSetting(const TreeNodeSettingsType &setting, const Value &value);
	//get the value of a metric
	Value GetSetting(const TreeNodeSettingsType &setting);
	//get the value of a metric
	Value GetSetting(const TreeNodeSettingsType &setting) const;

public:
	//set the metrics map
	void SetMetrics(unordered_map<TreeNodeSettingsType, Value> &n_metrics);
	//get the metrics map
	unordered_map<TreeNodeSettingsType, Value> &GetMetrics();

public:
	//reset the metrics to default
	void ResetMetrics();
	bool SettingEnabled(const TreeNodeSettingsType setting) const;
};
} // namespace duckdb
