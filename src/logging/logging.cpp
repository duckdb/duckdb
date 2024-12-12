#include "duckdb/logging/log_storage.hpp"
#include "duckdb/logging/logger.hpp"

#include "duckdb/common/types/column/column_data_collection.hpp"
#include "duckdb/common/types/data_chunk.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/main/connection.hpp"
#include "duckdb/main/database.hpp"
#include "duckdb/main/table_description.hpp"

#include <duckdb/common/file_opener.hpp>
#include <duckdb/parallel/thread_context.hpp>

namespace duckdb {

LogConfig::LogConfig() : enabled(false), mode(LogMode::LEVEL_ONLY), level(DEFAULT_LOG_LEVEL), storage(DEFAULT_LOG_STORAGE) {
}

bool LogConfig::IsConsistent() const {
	if (mode == LogMode::LEVEL_ONLY) {
		return enabled_loggers.empty() && disabled_loggers.empty();
	}
	if (mode == LogMode::DISABLE_SELECTED) {
		return enabled_loggers.empty() && !disabled_loggers.empty();
	}
	if (mode == LogMode::ENABLE_SELECTED) {
		return !enabled_loggers.empty() && disabled_loggers.empty();
	}
	return false;
}

LogConfig LogConfig::Create(bool enabled, LogLevel level) {
	return LogConfig(enabled, level, LogMode::LEVEL_ONLY, nullptr, nullptr);
}
LogConfig LogConfig::CreateFromEnabled(bool enabled, LogLevel level, unordered_set<string> &enabled_loggers) {
	return LogConfig(enabled, level, LogMode::ENABLE_SELECTED, enabled_loggers, nullptr);
}

LogConfig LogConfig::CreateFromDisabled(bool enabled, LogLevel level, unordered_set<string> &disabled_loggers) {
	return LogConfig(enabled, level, LogMode::DISABLE_SELECTED, nullptr, disabled_loggers);
}

LogConfig::LogConfig(bool enabled, LogLevel level_p, LogMode mode_p,
                     optional_ptr<unordered_set<string>> enabled_loggers_p,
                     optional_ptr<unordered_set<string>> disabled_loggers_p)
    : enabled(enabled), mode(mode_p), level(level_p), enabled_loggers(enabled_loggers_p),
      disabled_loggers(disabled_loggers_p) {
	storage = LogConfig::IN_MEMORY_STORAGE_NAME;
}

} // namespace duckdb
