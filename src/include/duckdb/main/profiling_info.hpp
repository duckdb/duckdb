//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/main/tree_node_settings.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/common.hpp"
#include "duckdb/common/enums/output_type.hpp"
#include "duckdb/common/enums/profiler_format.hpp"
#include "duckdb/common/progress_bar/progress_bar.hpp"
#include "duckdb/common/types/value.hpp"
#include "duckdb/common/unordered_set.hpp"
#include "duckdb/common/constants.hpp"

namespace duckdb {

enum class MetricsType : uint8_t { CPU_TIME, EXTRA_INFO, OPERATOR_CARDINALITY, OPERATOR_TIMING };

struct MetricsTypeHashFunction {
	uint64_t operator()(const MetricsType &index) const {
		return std::hash<uint8_t>()(static_cast<uint8_t>(index));
	}
};

typedef unordered_set<MetricsType, MetricsTypeHashFunction> profiler_settings_t;

struct SettingSetFunctions {
	static bool Enabled(const profiler_settings_t &settings, const MetricsType setting) {
		if (settings.find(setting) != settings.end()) {
			return true;
		}
		if (setting == MetricsType::OPERATOR_TIMING && Enabled(settings, MetricsType::CPU_TIME)) {
			return true;
		}
		return false;
	}
};

struct Metrics {
	double cpu_time;
	string extra_info;
	idx_t operator_cardinality;
	double operator_timing;

	Metrics() : cpu_time(0), operator_cardinality(0), operator_timing(0) {
	}
};

class ProfilingInfo {
public:
	// set of metrics with their values; only enabled metrics are present in the set
	profiler_settings_t settings;
	Metrics metrics;

public:
	ProfilingInfo() = default;
	explicit ProfilingInfo(profiler_settings_t &n_settings) : settings(n_settings) {
	}
	ProfilingInfo(ProfilingInfo &) = default;
	ProfilingInfo &operator=(ProfilingInfo const &) = default;

public:
	// set the metrics set
	void SetSettings(profiler_settings_t const &n_settings);
	// get the metrics set
	const profiler_settings_t &GetSettings();
	static profiler_settings_t DefaultSettings();

public:
	// reset the metrics to default
	void ResetSettings();
	void ResetMetrics();
	bool Enabled(const MetricsType setting) const;

public:
	string GetMetricAsString(MetricsType setting) const;
	void PrintAllMetricsToSS(std::ostream &ss, const string &depth);
};
} // namespace duckdb
