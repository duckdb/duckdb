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
private:
	// map of metrics with their values; only enabled metrics are present in the map
	unordered_set<TreeNodeSettingsType> metrics = default_metrics;
	profilingValues values;

public:
	TreeNodeSettings() = default;
	TreeNodeSettings(TreeNodeSettings &) = default;
	TreeNodeSettings &operator=(TreeNodeSettings const&) = default;

public:
	//set the metrics map
	void SetMetrics(unordered_set<TreeNodeSettingsType> &n_metrics);
	//get the metrics map
	unordered_set<TreeNodeSettingsType> &GetMetrics();

public:
	void SetCpuTime(double cpu_time);
	void AddToCpuTime(double cpu_time);
	double GetCpuTime() const;

	void SetExtraInfo(string extra_info);
	void AddToExtraInfo(string extra_info);
	string GetExtraInfo() const;

	void SetOperatorCardinality(idx_t operator_cardinality);
	void AddToOperatorCardinality(idx_t operator_cardinality);
	idx_t GetOperatorCardinality() const;

	void SetOperatorTiming(double operator_timing);
	void AddToOperatorTiming(double operator_timing);
	double GetOperatorTiming() const;

public:
	//reset the metrics to default
	void ResetMetrics();
	bool SettingEnabled(TreeNodeSettingsType setting) const;

public:
	string GetMetricAsString(TreeNodeSettingsType setting) const;
	void PrintAllMetricsToSS(std::ostream &ss, string depth);
};
} // namespace duckdb
