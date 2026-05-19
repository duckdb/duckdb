#include "duckdb/main/profiling_info.hpp"

#include "duckdb/common/enum_util.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/main/profiling_utils.hpp"
#include "duckdb/main/query_profiler.hpp"
#include "duckdb/logging/log_manager.hpp"

#include "yyjson.hpp"

using namespace duckdb_yyjson; // NOLINT

namespace duckdb {

ProfilingInfo::ProfilingInfo(const profiler_settings_t &n_settings, const idx_t depth) : settings(n_settings) {
	// Expand.
	if (depth > 0) {
		settings.insert("OPERATOR_NAME");
		settings.insert("OPERATOR_TYPE");
	}
	for (const auto &metric : settings) {
		if (MetricsUtils::IsOptimizerMetricKey(metric) || MetricsUtils::IsStorageMetricKey(metric)) {
			expanded_settings.insert(metric);
			continue;
		}
		Expand(expanded_settings, EnumUtil::FromString<MetricType>(metric));
	}

	// Reduce.
	if (depth == 0) {
		auto op_metrics = MetricsUtils::GetOperatorMetrics();
		for (const auto &metric : op_metrics) {
			settings.erase(metric);
		}
	} else {
		auto root_metrics = MetricsUtils::GetRootScopeMetrics();
		for (const auto &metric : root_metrics) {
			settings.erase(metric);
		}
	}
	ResetMetrics();
}

void ProfilingInfo::ResetMetrics() {
	metrics.clear();
	for (const auto &metric : expanded_settings) {
		if (MetricsUtils::IsOptimizerMetricKey(metric)) {
			metrics[metric] = Value::CreateValue(0.0);
			continue;
		}
		if (MetricsUtils::IsStorageMetricKey(metric)) {
			if (MetricsUtils::IsStorageTimerKey(metric)) {
				metrics[metric] = Value::CreateValue(0.0);
			} else {
				metrics[metric] = Value::CreateValue<uint64_t>(0);
			}
			continue;
		}
		auto metric_type = EnumUtil::FromString<MetricType>(metric);
		if (MetricsUtils::IsPhaseTimingMetric(metric_type)) {
			metrics[metric] = Value::CreateValue(0.0);
			continue;
		}
		ProfilingUtils::SetMetricToDefault(metrics, metric_type);
	}
}

bool ProfilingInfo::Enabled(const MetricType metric) const {
	return settings.find(EnumUtil::ToString(metric)) != settings.end();
}

bool ProfilingInfo::EnabledForCollection(const MetricType metric) const {
	return expanded_settings.find(EnumUtil::ToString(metric)) != expanded_settings.end();
}

void ProfilingInfo::Expand(profiler_settings_t &settings, const MetricType metric) {
	settings.insert(EnumUtil::ToString(metric));

	switch (metric) {
	case MetricType::CPU_TIME:
		settings.insert(EnumUtil::ToString(MetricType::OPERATOR_TIMING));
		return;
	case MetricType::CUMULATIVE_CARDINALITY:
		settings.insert(EnumUtil::ToString(MetricType::OPERATOR_CARDINALITY));
		return;
	case MetricType::CUMULATIVE_ROWS_SCANNED:
		settings.insert(EnumUtil::ToString(MetricType::OPERATOR_ROWS_SCANNED));
		return;
	case MetricType::CUMULATIVE_OPTIMIZER_TIMING:
	case MetricType::ALL_OPTIMIZERS: {
		auto optimizer_metrics = MetricsUtils::GetOptimizerMetrics();
		for (const auto &optimizer_metric : optimizer_metrics) {
			settings.insert(optimizer_metric);
		}
		return;
	}
	default:
		break;
	}
}

void ProfilingInfo::SetMetricValue(MetricType type, Value new_value) {
	metrics[EnumUtil::ToString(type)] = std::move(new_value);
}

void ProfilingInfo::WriteMetricsToLog(ClientContext &context) const {
	auto &logger = Logger::Get(context);
	if (logger.ShouldLog(MetricsLogType::NAME, MetricsLogType::LEVEL)) {
		for (const auto &metric : settings) {
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
	// Group dotted metric keys (e.g. "optimizer.join_order") into nested result objects.
	unordered_map<string, reference<QueryProfileResult>> groups;

	for (auto &entry : metrics) {
		if (settings.find(entry.first) == settings.end()) {
			continue;
		}
		auto dot_pos = entry.first.find('.');
		if (dot_pos != string::npos) {
			auto prefix = entry.first.substr(0, dot_pos);
			auto suffix = entry.first.substr(dot_pos + 1);
			auto it = groups.find(prefix);
			if (it == groups.end()) {
				auto &obj = result.AddObject(prefix);
				groups.emplace(prefix, obj);
				obj.AddValue(suffix, entry.second);
			} else {
				it->second.get().AddValue(suffix, entry.second);
			}
		} else {
			result.AddValue(StringUtil::Lower(entry.first), entry.second);
		}
	}
}

} // namespace duckdb
