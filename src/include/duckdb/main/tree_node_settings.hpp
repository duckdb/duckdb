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
#include "duckdb/common/unordered_set.hpp"
#include "duckdb/common/constants.hpp"
#

namespace duckdb {


enum class TreeNodeSettingsType : uint8_t { CPU_TIME, EXTRA_INFO, OPERATOR_CARDINALITY, OPERATOR_TIMING };

struct TreeNodeSettingsTypeHashFunction {
	uint64_t operator()(const TreeNodeSettingsType &index) const {
		return std::hash<uint8_t>()(static_cast<uint8_t>(index));
	}
};

const unordered_set<TreeNodeSettingsType> default_metrics = {
    TreeNodeSettingsType::CPU_TIME,
    TreeNodeSettingsType::EXTRA_INFO,
    TreeNodeSettingsType::OPERATOR_CARDINALITY,
    TreeNodeSettingsType::OPERATOR_TIMING,
};

struct profilingValues {
	double cpu_time;
	string extra_info;
	idx_t operator_cardinality;
	double operator_timing;
};

class TreeNodeSettings {
public:
	// map of metrics with their values; only enabled metrics are present in the map
	unordered_set<TreeNodeSettingsType> metrics = default_metrics;
	profilingValues values;

public:
	TreeNodeSettings() = default;
	TreeNodeSettings(TreeNodeSettings &) = default;
	TreeNodeSettings &operator=(TreeNodeSettings const &) = default;

public:
	// set the metrics map
	void SetMetrics(unordered_set<TreeNodeSettingsType> &n_metrics);
	// get the metrics map
	unordered_set<TreeNodeSettingsType> &GetMetrics();

public:
	// reset the metrics to default
	void ResetMetrics();
	void ResetValues();
	bool SettingEnabled(TreeNodeSettingsType setting) const;

public:
	string GetMetricAsString(TreeNodeSettingsType setting) const;
	void PrintAllMetricsToSS(std::ostream &ss, string depth);
};
} // namespace duckdb
