#include "duckdb/logging/log_sink.hpp"
#include "duckdb/logging/logger.hpp"

namespace duckdb {

LogConfig::LogConfig()
    : enabled(false), mode(LogMode::LEVEL_ONLY), level(DEFAULT_LOG_LEVEL), storage(DEFAULT_LOG_STORAGE) {
}

bool LogConfig::IsConsistent() const {
	if (mode == LogMode::LEVEL_ONLY) {
		return enabled_log_types.empty() && disabled_log_types.empty();
	}
	if (mode == LogMode::DISABLE_SELECTED) {
		return enabled_log_types.empty() && !disabled_log_types.empty();
	}
	if (mode == LogMode::ENABLE_SELECTED) {
		return !enabled_log_types.empty() && disabled_log_types.empty();
	}
	return false;
}

bool LogConfig::Accepts(const char *log_type, LogLevel log_level) const {
	if (!enabled || level > log_level) {
		return false;
	}
	if (mode == LogMode::ENABLE_SELECTED) {
		return enabled_log_types.find(log_type) != enabled_log_types.end();
	}
	if (mode == LogMode::DISABLE_SELECTED) {
		return disabled_log_types.find(log_type) == disabled_log_types.end();
	}
	return true;
}

bool LogConfig::IsBuiltInLogSink(const string &sink_name) {
	return sink_name == IN_MEMORY_STORAGE_NAME || sink_name == STDOUT_STORAGE_NAME || sink_name == FILE_STORAGE_NAME;
}

LogConfig::LogConfig(bool enabled, LogLevel level_p, LogMode mode_p,
                     optional_ptr<unordered_set<string>> enabled_log_types_p,
                     optional_ptr<unordered_set<string>> disabled_log_types_p)
    : enabled(enabled), mode(mode_p), level(level_p), enabled_log_types(enabled_log_types_p),
      disabled_log_types(disabled_log_types_p) {
	storage = LogConfig::IN_MEMORY_STORAGE_NAME;
}

} // namespace duckdb
