//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/logging/logger.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/logging/logging.hpp"
#include "duckdb/common/types.hpp"
#include "duckdb/common/string_util.hpp"

#include <functional>

namespace duckdb {
class TableDescription;
class DatabaseInstance;
class DataChunk;
class LogManager;
class ColumnDataCollection;
class ThreadContext;
class FileOpener;
class LogStorage;
class ExecutionContext;

//! Main logging interface
class Logger {
public:
	DUCKDB_API explicit Logger(LogManager &manager) : manager(manager) {
	}

	DUCKDB_API virtual ~Logger() = default;

	//! Main logger functions
	DUCKDB_API void Log(const char *log_type, LogLevel log_level, const char *log_message);
	DUCKDB_API void Log(LogLevel log_level, const char *log_message);
	DUCKDB_API void Log(const char *log_type, LogLevel log_level, const string_t &log_message);
	DUCKDB_API void Log(LogLevel log_level, const string_t &log_message);
	DUCKDB_API void Log(const char *log_type, LogLevel log_level, std::function<string()> callback);
	DUCKDB_API void Log(LogLevel log_level, std::function<string()> callback);

	// Main interface for subclasses
	DUCKDB_API virtual bool ShouldLog(const char *log_type, LogLevel log_level) = 0;
	DUCKDB_API virtual bool ShouldLog(LogLevel log_level) = 0;
	DUCKDB_API virtual void WriteLog(const char *log_type, LogLevel log_level, const char *message) = 0;
	DUCKDB_API virtual void WriteLog(LogLevel log_level, const char *message) = 0;

	DUCKDB_API virtual void Flush() = 0;

	// Get the Logger to write log messages to. In decreasing order of preference(!) so the ThreadContext getter is the
	// most preferred way of fetching the logger and the DatabaseInstance getter the least preferred. This has to do
	// both with logging performance and level of detail of logging context that is provided.
	DUCKDB_API static Logger &Get(const ThreadContext &thread_context);
	DUCKDB_API static Logger &Get(const ExecutionContext &execution_context);
	DUCKDB_API static Logger &Get(const ClientContext &client_context);
	DUCKDB_API static Logger &Get(const FileOpener &opener);
	DUCKDB_API static Logger &Get(const DatabaseInstance &db);

	//! Logger::Log with raw C-String
	template <class T>
	static void Log(const char *log_type, T &log_context_source, LogLevel log_level, const char *log_message) {
		Logger::Get(log_context_source).Log(log_type, log_level, log_message);
	}
	template <class T>
	static void Log(const char *log_type, T &log_context_source, LogLevel log_level, const string &log_message) {
		Logger::Get(log_context_source).Log(log_type, log_level, log_message.c_str());
	}
	template <class T>
	static void Log(const char *log_type, T &log_context_source, LogLevel log_level, const string_t &log_message) {
		Logger::Get(log_context_source).Log(log_type, log_level, log_message);
	}
	template <class T>
	static void Log(T &log_context_source, LogLevel log_level, const char *log_message) {
		Logger::Get(log_context_source).Log(log_level, log_message);
	}
	template <class T>
	static void Log(T &log_context_source, LogLevel log_level, const string &log_message) {
		Logger::Get(log_context_source).Log(log_level, log_message.c_str());
	}
	template <class T>
	static void Log(T &log_context_source, LogLevel log_level, const string_t &log_message) {
		Logger::Get(log_context_source).Log(log_level, log_message);
	}

	//! Logger::Log with callback
	template <class T>
	static void Log(const char *log_type, T &log_context_source, LogLevel log_level, std::function<string()> callback) {
		Logger::Get(log_context_source).Log(log_type, log_level, std::move(callback));
	}
	template <class T>
	static void Log(T &log_context_source, LogLevel log_level, std::function<string()> callback) {
		Logger::Get(log_context_source).Log(log_level, std::move(callback));
	}
	//! Logger::Log with StringUtil::Format
	template <class T, typename... ARGS>
	static void Log(const char *log_type, T &log_context_source, LogLevel log_level, const char *format_string,
	                ARGS... params) {
		Logger::Get(log_context_source).Log(log_type, log_level, [&]() {
			return StringUtil::Format(format_string, params...);
		});
	}
	template <class T, typename... ARGS>
	static void Log(T &log_context_source, LogLevel log_level, const char *format_string, ARGS... params) {
		Logger::Get(log_context_source).Log(log_level, [&]() { return StringUtil::Format(format_string, params...); });
	}

	//! Templates wrapping Logging::Log(..., LoggingLevel, ....)
	template <class T, typename... ARGS>
	static void Trace(T &log_context_source, ARGS... params) {
		Log(log_context_source, LogLevel::LOG_TRACE, params...);
	}
	template <class T, typename... ARGS>
	static void Trace(const char *log_type, T &log_context_source, ARGS... params) {
		Log(log_type, log_context_source, LogLevel::LOG_TRACE, params...);
	}
	template <class T, typename... ARGS>
	static void Debug(T &log_context_source, ARGS... params) {
		Log(log_context_source, LogLevel::LOG_DEBUG, params...);
	}
	template <class T, typename... ARGS>
	static void Debug(const char *log_type, T &log_context_source, ARGS... params) {
		Log(log_type, log_context_source, LogLevel::LOG_DEBUG, params...);
	}
	template <class T, typename... ARGS>
	static void Info(T &log_context_source, ARGS... params) {
		Log(log_context_source, LogLevel::LOG_INFO, params...);
	}
	template <class T, typename... ARGS>
	static void Info(const char *log_type, T &log_context_source, ARGS... params) {
		Log(log_type, log_context_source, LogLevel::LOG_INFO, params...);
	}
	template <class T, typename... ARGS>
	static void Warn(T &log_context_source, ARGS... params) {
		Log(log_context_source, LogLevel::LOG_WARN, params...);
	}
	template <class T, typename... ARGS>
	static void Warn(const char *log_type, T &log_context_source, ARGS... params) {
		Log(log_type, log_context_source, LogLevel::LOG_WARN, params...);
	}
	template <class T, typename... ARGS>
	static void Error(T &log_context_source, ARGS... params) {
		Log(log_context_source, LogLevel::LOG_ERROR, params...);
	}
	template <class T, typename... ARGS>
	static void Error(const char *log_type, T &log_context_source, ARGS... params) {
		Log(log_type, log_context_source, LogLevel::LOG_ERROR, params...);
	}
	template <class T, typename... ARGS>
	static void Fatal(T &log_context_source, ARGS... params) {
		Log(log_context_source, LogLevel::LOG_FATAL, params...);
	}
	template <class T, typename... ARGS>
	static void Fatal(const char *log_type, T &log_context_source, ARGS... params) {
		Log(log_type, log_context_source, LogLevel::LOG_FATAL, params...);
	}

	template <class T>
	static void Flush(T &log_context_source) {
		Get(log_context_source).Flush();
	}

	DUCKDB_API virtual bool IsThreadSafe() = 0;
	DUCKDB_API virtual bool IsMutable() {
		return false;
	};
	DUCKDB_API virtual void UpdateConfig(LogConfig &new_config) {
		throw InternalException("Cannot update the config of this logger!");
	}
	DUCKDB_API virtual const LogConfig &GetConfig() const = 0;

protected:
	LogManager &manager;
};

// Thread-safe logger
class ThreadSafeLogger : public Logger {
public:
	explicit ThreadSafeLogger(LogConfig &config_p, LoggingContext &context_p, LogManager &manager);
	explicit ThreadSafeLogger(LogConfig &config_p, RegisteredLoggingContext context_p, LogManager &manager);

	// Main Logger API
	bool ShouldLog(const char *log_type, LogLevel log_level) override;
	bool ShouldLog(LogLevel log_level) override;
	void WriteLog(const char *log_type, LogLevel log_level, const char *message) override;
	void WriteLog(LogLevel log_level, const char *message) override;

	void Flush() override;
	bool IsThreadSafe() override {
		return true;
	}
	const LogConfig &GetConfig() const override {
		return config;
	}

protected:
	const LogConfig config;
	mutex lock;
	const RegisteredLoggingContext context;
};

// Non Thread-safe logger
// - will cache log entries locally
class ThreadLocalLogger : public Logger {
public:
	explicit ThreadLocalLogger(LogConfig &config_p, LoggingContext &context_p, LogManager &manager);
	explicit ThreadLocalLogger(LogConfig &config_p, RegisteredLoggingContext context_p, LogManager &manager);

	// Main Logger API
	bool ShouldLog(const char *log_type, LogLevel log_level) override;
	bool ShouldLog(LogLevel log_level) override;
	void WriteLog(const char *log_type, LogLevel log_level, const char *message) override;
	void WriteLog(LogLevel log_level, const char *message) override;
	void Flush() override;

	bool IsThreadSafe() override {
		return false;
	}
	const LogConfig &GetConfig() const override {
		return config;
	}

protected:
	const LogConfig config;
	const RegisteredLoggingContext context;
};

// Thread-safe Logger with mutable log settings
class MutableLogger : public Logger {
public:
	explicit MutableLogger(LogConfig &config_p, LoggingContext &context_p, LogManager &manager);
	explicit MutableLogger(LogConfig &config_p, RegisteredLoggingContext context_p, LogManager &manager);

	// Main Logger API
	bool ShouldLog(const char *log_type, LogLevel log_level) override;
	bool ShouldLog(LogLevel log_level) override;
	void WriteLog(const char *log_type, LogLevel log_level, const char *message) override;
	void WriteLog(LogLevel log_level, const char *message) override;

	void Flush() override;
	bool IsThreadSafe() override {
		return true;
	}
	bool IsMutable() override {
		return true;
	}
	const LogConfig &GetConfig() const override {
		return config;
	}
	void UpdateConfig(LogConfig &new_config) override;

protected:
	// Atomics for lock-free log setting checks
	atomic<bool> enabled;
	atomic<LogMode> mode;
	atomic<LogLevel> level;

	mutex lock;
	LogConfig config;
	const RegisteredLoggingContext context;
};

// For when logging is disabled: NOPs everything
class NopLogger : public Logger {
public:
	explicit NopLogger(LogManager &manager) : Logger(manager) {
	}
	bool ShouldLog(const char *log_type, LogLevel log_level) override {
		return false;
	}
	bool ShouldLog(LogLevel log_level) override {
		return false;
	};
	void WriteLog(const char *log_type, LogLevel log_level, const char *message) override {};
	void WriteLog(LogLevel log_level, const char *message) override {};
	void Flush() override {
	}
	bool IsThreadSafe() override {
		return true;
	}
	const LogConfig &GetConfig() const override {
		throw InternalException("Called GetConfig on NopLogger");
	}
};

} // namespace duckdb
