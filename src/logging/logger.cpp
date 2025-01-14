#include "duckdb/logging/log_storage.hpp"
#include "duckdb/logging/log_manager.hpp"

#include "duckdb/main/client_context.hpp"
#include "duckdb/main/connection.hpp"
#include "duckdb/main/database.hpp"
#include "duckdb/common/file_opener.hpp"
#include "duckdb/parallel/thread_context.hpp"

namespace duckdb {

Logger &Logger::Get(const DatabaseInstance &db) {
	return db.GetLogManager().GlobalLogger();
}

Logger &Logger::Get(const ThreadContext &thread_context) {
	return *thread_context.logger;
}

Logger &Logger::Get(const ExecutionContext &execution_context) {
	return *execution_context.thread.logger;
}

Logger &Logger::Get(const ClientContext &client_context) {
	return client_context.GetLogger();
}

Logger &Logger::Get(const FileOpener &opener) {
	return opener.GetLogger();
}

void Logger::Log(const char *log_type, LogLevel log_level, const char *log_message) {
	if (ShouldLog(log_type, log_level)) {
		WriteLog(log_type, log_level, log_message);
	}
}

void Logger::Log(LogLevel log_level, const char *log_message) {
	if (ShouldLog(log_level)) {
		WriteLog(log_level, log_message);
	}
}

void Logger::Log(const char *log_type, LogLevel log_level, const string_t &log_message) {
	if (ShouldLog(log_type, log_level)) {
		auto string_copy = log_message.GetString();
		WriteLog(log_type, log_level, string_copy.c_str());
	}
}

void Logger::Log(LogLevel log_level, const string_t &log_message) {
	if (ShouldLog(log_level)) {
		auto string_copy = log_message.GetString();
		WriteLog(log_level, string_copy.c_str());
	}
}

void Logger::Log(const char *log_type, LogLevel log_level, std::function<string()> callback) { // NOLINT
	if (ShouldLog(log_type, log_level)) {
		auto string = callback();
		WriteLog(log_type, log_level, string.c_str());
	}
}

void Logger::Log(LogLevel log_level, std::function<string()> callback) { // NOLINT
	if (ShouldLog(log_level)) {
		auto string = callback();
		WriteLog(log_level, string.c_str());
	}
}

ThreadSafeLogger::ThreadSafeLogger(LogConfig &config_p, LoggingContext &context_p, LogManager &manager)
    : ThreadSafeLogger(config_p, manager.RegisterLoggingContext(context_p), manager) {
}

ThreadSafeLogger::ThreadSafeLogger(LogConfig &config_p, RegisteredLoggingContext context_p, LogManager &manager)
    : Logger(manager), config(config_p), context(context_p) {
	// NopLogger should be used instead
	D_ASSERT(config_p.enabled);
}

bool ThreadSafeLogger::ShouldLog(const char *log_type, LogLevel log_level) {
	if (config.level > log_level) {
		return false;
	}

	// TODO: improve these: they are currently fairly expensive due to requiring allocations when looking up const char*
	//       also, we would ideally do prefix matching, not string matching here
	if (config.mode == LogMode::ENABLE_SELECTED) {
		return config.enabled_log_types.find(log_type) != config.enabled_log_types.end();
	}
	if (config.mode == LogMode::DISABLE_SELECTED) {
		return config.disabled_log_types.find(log_type) == config.disabled_log_types.end();
	}
	return true;
}

bool ThreadSafeLogger::ShouldLog(LogLevel log_level) {
	return ShouldLog(context.context.default_log_type, log_level);
}

void ThreadSafeLogger::WriteLog(const char *log_type, LogLevel log_level, const char *log_message) {
	manager.WriteLogEntry(Timestamp::GetCurrentTimestamp(), log_type, log_level, log_message, context);
}

void ThreadSafeLogger::WriteLog(LogLevel log_level, const char *log_message) {
	manager.WriteLogEntry(Timestamp::GetCurrentTimestamp(), context.context.default_log_type, log_level, log_message,
	                      context);
}

void ThreadSafeLogger::Flush() {
	manager.Flush();
	// NOP
}

ThreadLocalLogger::ThreadLocalLogger(LogConfig &config_p, LoggingContext &context_p, LogManager &manager)
    : ThreadLocalLogger(config_p, manager.RegisterLoggingContext(context_p), manager) {
}

ThreadLocalLogger::ThreadLocalLogger(LogConfig &config_p, RegisteredLoggingContext context_p, LogManager &manager)
    : Logger(manager), config(config_p), context(context_p) {
	// NopLogger should be used instead
	D_ASSERT(config_p.enabled);
}

bool ThreadLocalLogger::ShouldLog(const char *log_type, LogLevel log_level) {
	throw NotImplementedException("ThreadLocalLogger::ShouldLog");
}

bool ThreadLocalLogger::ShouldLog(LogLevel log_level) {
	throw NotImplementedException("ThreadLocalLogger::ShouldLog");
}

void ThreadLocalLogger::WriteLog(const char *log_type, LogLevel log_level, const char *log_message) {
	throw NotImplementedException("ThreadLocalLogger::WriteLog");
}

void ThreadLocalLogger::WriteLog(LogLevel log_level, const char *log_message) {
	throw NotImplementedException("ThreadLocalLogger::WriteLog");
}

void ThreadLocalLogger::Flush() {
	manager.Flush();
}

MutableLogger::MutableLogger(LogConfig &config_p, LoggingContext &context_p, LogManager &manager)
    : MutableLogger(config_p, manager.RegisterLoggingContext(context_p), manager) {
}

MutableLogger::MutableLogger(LogConfig &config_p, RegisteredLoggingContext context_p, LogManager &manager)
    : Logger(manager), config(config_p), context(context_p) {
	enabled = config.enabled;
	level = config.level;
	mode = config.mode;
}

void MutableLogger::UpdateConfig(LogConfig &new_config) {
	unique_lock<mutex> lck(lock);
	config = new_config;

	// Update atomics for lock-free access
	enabled = config.enabled;
	level = config.level;
	mode = config.mode;
}

void MutableLogger::WriteLog(const char *log_type, LogLevel log_level, const char *log_message) {
	manager.WriteLogEntry(Timestamp::GetCurrentTimestamp(), log_type, log_level, log_message, context);
}

void MutableLogger::WriteLog(LogLevel log_level, const char *log_message) {
	manager.WriteLogEntry(Timestamp::GetCurrentTimestamp(), context.context.default_log_type, log_level, log_message,
	                      context);
}

bool MutableLogger::ShouldLog(const char *log_type, LogLevel log_level) {
	if (!enabled) {
		return false;
	}

	// check atomic level to early out if level too low
	if (level > log_level) {
		return false;
	}

	if (mode == LogMode::LEVEL_ONLY) {
		return true;
	}

	// FIXME: ENABLE_SELECTED and DISABLE_SELECTED are expensive and need full global lock
	{
		unique_lock<mutex> lck(lock);
		if (config.mode == LogMode::ENABLE_SELECTED) {
			return config.enabled_log_types.find(log_type) != config.enabled_log_types.end();
		}
		if (config.mode == LogMode::DISABLE_SELECTED) {
			return config.disabled_log_types.find(log_type) == config.disabled_log_types.end();
		}
	}
	throw InternalException("Should be unreachable (MutableLogger::ShouldLog)");
}

bool MutableLogger::ShouldLog(LogLevel log_level) {
	return ShouldLog(context.context.default_log_type, log_level);
}

void MutableLogger::Flush() {
	manager.Flush();
}

} // namespace duckdb
