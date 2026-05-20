#include "duckdb/main/profiling_info.hpp"

#include "duckdb/common/string_util.hpp"
#include "duckdb/main/profiling_utils.hpp"
#include "duckdb/main/query_profiler.hpp"
#include "duckdb/logging/log_manager.hpp"

#include "yyjson.hpp"

using namespace duckdb_yyjson; // NOLINT

namespace duckdb {

ProfilingInfo::ProfilingInfo(const profiler_settings_t &n_settings) : settings(n_settings) {
	ResetMetrics();
}

void ProfilingInfo::ResetMetrics() {
	metrics.clear();
}

bool ProfilingInfo::EnabledForCollection(const string &key) const {
	return settings.find(key) != settings.end();
}

void ProfilingInfo::SetMetricValue(const string &key, Value new_value) {
	if (!EnabledForCollection(key)) {
		return;
	}
	metrics[key] = std::move(new_value);
}

void ProfilingInfo::SetMetricValue(const string &key, idx_t value) {
	if (!EnabledForCollection(key)) {
		return;
	}
	metrics[key] = Value::UBIGINT(value);
}

void ProfilingInfo::SetMetricValue(const string &key, double value) {
	if (!EnabledForCollection(key)) {
		return;
	}
	metrics[key] = Value::DOUBLE(value);
}

void ProfilingInfo::SetMetricValue(const string &key, const string &value) {
	if (!EnabledForCollection(key)) {
		return;
	}
	metrics[key] = Value(value);
}

void ProfilingInfo::WriteMetricsToLog(ClientContext &context) const {
	auto &logger = Logger::Get(context);
	if (logger.ShouldLog(MetricsLogType::NAME, MetricsLogType::LEVEL)) {
		for (const auto &metric : settings) {
			auto entry = metrics.find(metric);
			if (entry == metrics.end()) {
				continue; // Metric was not recorded this query
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
