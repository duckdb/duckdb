#include "duckdb/main/profiling_info.hpp"

#include "duckdb/common/enum_util.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/main/profiling_utils.hpp"
#include "duckdb/main/query_profiler.hpp"
#include "duckdb/logging/log_manager.hpp"

#include "yyjson.hpp"

using namespace duckdb_yyjson; // NOLINT

namespace duckdb {

ProfilerSettings::ProfilerSettings(profiler_settings_t settings_p) : settings(std::move(settings_p)) {
}

bool ProfilerSettings::MetricIsEnabled(MetricType metric) const {
	return settings.find(metric) != settings.end();
}

ProfilingInfo::ProfilingInfo(const profiler_settings_t &n_settings, const idx_t depth) : settings(n_settings) {
	// Expand.
	if (depth > 0) {
		settings.insert(MetricType::OPERATOR_NAME);
		settings.insert(MetricType::OPERATOR_TYPE);
	}
	for (const auto &metric : settings) {
		Expand(expanded_settings, metric);
	}

	// Reduce.
	if (depth == 0) {
		auto op_metrics = MetricsUtils::GetOperatorMetrics();
		for (const auto metric : op_metrics) {
			settings.erase(metric);
		}
	} else {
		auto root_metrics = MetricsUtils::GetRootScopeMetrics();
		for (const auto metric : root_metrics) {
			settings.erase(metric);
		}
	}
	ResetMetrics();
}

void ProfilingInfo::ResetMetrics() {
	metrics.clear();
	for (auto &metric : expanded_settings) {
		if (MetricsUtils::IsOptimizerMetric(metric) || MetricsUtils::IsPhaseTimingMetric(metric)) {
			metrics[metric] = Value::CreateValue(0.0);
			continue;
		}

		ProfilingUtils::SetMetricToDefault(metrics, metric);
	}
}

bool ProfilingInfo::Enabled(const MetricType metric) const {
	return settings.find(metric) != settings.end();
}

bool ProfilingInfo::EnabledForCollection(const MetricType metric) const {
	return expanded_settings.find(metric) != expanded_settings.end();
}

void ProfilingInfo::Expand(profiler_settings_t &settings, const MetricType metric) {
	settings.insert(metric);

	switch (metric) {
	case MetricType::CPU_TIME:
		settings.insert(MetricType::OPERATOR_TIMING);
		return;
	case MetricType::CUMULATIVE_CARDINALITY:
		settings.insert(MetricType::OPERATOR_CARDINALITY);
		return;
	case MetricType::CUMULATIVE_ROWS_SCANNED:
		settings.insert(MetricType::OPERATOR_ROWS_SCANNED);
		return;
	case MetricType::CUMULATIVE_OPTIMIZER_TIMING:
	case MetricType::ALL_OPTIMIZERS: {
		auto optimizer_metrics = MetricsUtils::GetOptimizerMetrics();
		for (const auto optimizer_metric : optimizer_metrics) {
			settings.insert(optimizer_metric);
		}
		return;
	}
	default:
		break;
	}
}

void ProfilingInfo::SetMetricValue(MetricType type, Value new_value) {
	metrics[type] = std::move(new_value);
}

string ProfilingInfo::GetMetricAsString(const MetricType metric) const {
	if (!Enabled(metric)) {
		throw InternalException("Metric %s not enabled", EnumUtil::ToString(metric));
	}

	// The metric cannot be NULL and must be initialized.
	D_ASSERT(!metrics.at(metric).IsNull());
	return metrics.at(metric).ToString();
}

void ProfilingInfo::WriteMetricsToLog(ClientContext &context) const {
	auto &logger = Logger::Get(context);
	if (logger.ShouldLog(MetricsLogType::NAME, MetricsLogType::LEVEL)) {
		for (auto &metric : settings) {
			auto entry = metrics.find(metric);
			if (entry == metrics.end()) {
				throw InternalException("Metric not instantiated correctly");
			}
			logger.WriteLog(MetricsLogType::NAME, MetricsLogType::LEVEL,
			                MetricsLogType::ConstructLogMessage(metric, entry->second));
		}
	}
}

void ProfilingInfo::MetricsToProfileResult(QueryProfileResult &result) const {
	for (auto &entry : metrics) {
		if (settings.find(entry.first) == settings.end()) {
			continue;
		}
		auto key = StringUtil::Lower(EnumUtil::ToString(entry.first));
		result.AddValue(key, entry.second);
	}
}

} // namespace duckdb
