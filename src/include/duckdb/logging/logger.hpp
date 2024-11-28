//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/logging/logger.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/atomic.hpp"
#include "duckdb/common/mutex.hpp"
#include "duckdb/common/optional_idx.hpp"
#include "duckdb/common/types.hpp"
#include "duckdb/common/unordered_set.hpp"

#include <duckdb/parallel/thread_context.hpp>

namespace duckdb {
class TableDescription;
class DatabaseInstance;
class DataChunk;
class LogManager;
class ColumnDataCollection;
class ThreadContext;
class FileOpener;

enum class LogLevel : uint8_t {
	DEBUGGING = 10,
	INFORMATIVE = 20,
	WARNING = 30,
	ERROR = 40,
	FATAL = 50,
};

struct LoggingContext {
	// TODO: potentially we might want to add stuff to identify which connection and thread the log came from
	optional_idx thread_id;     // maybe: TaskScheduler::GetEstimatedCPUId ?
	optional_idx connection_id; // maybe: use the address of the connection pointer
	optional_idx query_id;
	optional_idx transaction_id;

	const char *default_log_type = "";
};

// TODO: class ColumnDataCollectionLogStorage : public LogStorage
class LogStorage {
public:
	explicit LogStorage(shared_ptr<DatabaseInstance> &db);
	~LogStorage();

	void WriteLogEntry(LogLevel level, const string &log_type, const string &log_message,
	                   const LoggingContext &context);
	void WriteLogEntries(DataChunk &chunk, const LoggingContext &context);
	void Flush();

	unique_ptr<ColumnDataCollection> log_entries;

private:
	// Cache for direct logging
	unique_ptr<DataChunk> buffer;
	idx_t max_buffer_size;
};

enum class LogMode : uint8_t {
	DISABLED = 0,
	LEVEL_ONLY = 1,
	DISABLE_SELECTED = 2,
	ENABLE_SELECTED = 3,
};

enum class LogDestinationType : uint8_t {
	IN_MEMORY = 0,
};

struct LogConfig {
	LogConfig();

	static LogConfig Create(LogLevel level);
	static LogConfig CreateFromEnabled(LogLevel level, unordered_set<string> &enabled_loggers);
	static LogConfig CreateFromDisabled(LogLevel level, unordered_set<string> &disabled_loggers);

	LogMode Mode() const {
		return mode;
	}
	LogLevel Level() const {
		return level;
	}
	const unordered_set<string> &EnabledLoggers() const {
		return enabled_loggers;
	}
	const unordered_set<string> &DisabledLoggers() const {
		return disabled_loggers;
	}

	bool IsConsistent() const;

protected:
	LogConfig(LogLevel level, LogMode mode, optional_ptr<unordered_set<string>> enabled_loggers,
	          optional_ptr<unordered_set<string>> disable_loggers);

	LogMode mode;
	LogLevel level;
	LogDestinationType output;
	unordered_set<string> enabled_loggers;
	unordered_set<string> disabled_loggers;
};

//! Main logging interface
class Logger {
public:
	explicit Logger() {
	}
	explicit Logger(LogManager &manager) : manager(manager) {
	}

	virtual ~Logger() = default;

	//! Main logger functions (can be overridden for NopLogger)
	virtual void Log(const char *log_type, LogLevel log_level, const char *log_message);
	virtual void Log(LogLevel log_level, const char *log_message);
	virtual void Log(const char *log_type, LogLevel log_level, std::function<string()>);
	virtual void Log(LogLevel log_level, std::function<string()>);

	// Main interface for subclasses
	virtual bool ShouldLog(const char *log_type, LogLevel log_level) = 0;
	virtual bool ShouldLog(LogLevel log_level) = 0;
	virtual void WriteLog(const char *log_type, LogLevel log_level, const char *message) = 0;
	virtual void WriteLog(LogLevel log_level, const char *message) = 0;

	virtual void Flush() = 0;

	// Get the Logger to write log messages to. In decreasing order of preference(!) so the ThreadContext getter is the
	// most preferred way of fetching the logger and the DatabaseInstance getter the least preferred. This has to do
	// both with logging performance and level of detail of logging context that is provided.
	static Logger &Get(ThreadContext &thread_context);
	static Logger &Get(ClientContext &client_context);
	static Logger &Get(FileOpener &opener);
	static Logger &Get(DatabaseInstance &db);

	//! Logger::Log with raw C-String
	template <class T>
	static void Log(const char *log_type, T &log_context_source, LogLevel log_level, const char *log_message) {
		Logger::Get(log_context_source).Log(log_level, log_type, log_message);
	}
	template <class T>
	static void Log(T &log_context_source, LogLevel log_level, const char *log_message) {
		Logger::Get(log_context_source).Log(log_level, log_message);
	}
	//! Logger::Log with callback
	template <class T>
	static void Log(const char *log_type, T &log_context_source, LogLevel log_level, std::function<string()> callback) {
		Logger::Get(log_context_source).Log(log_type, log_level, callback);
	}
	template <class T>
	static void Log(T &log_context_source, LogLevel log_level, std::function<string()> callback) {
		Logger::Get(log_context_source).Log(log_level, callback);
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
	static void Debug(T &log_context_source, ARGS... params) {
		Log(log_context_source, LogLevel::DEBUGGING, params...);
	}
	template <class T, typename... ARGS>
	static void Debug(const char *log_type, T &log_context_source, ARGS... params) {
		Log(log_type, log_context_source, LogLevel::DEBUGGING, params...);
	}
	template <class T, typename... ARGS>
	static void Info(T &log_context_source, ARGS... params) {
		Log(log_context_source, LogLevel::INFORMATIVE, params...);
	}
	template <class T, typename... ARGS>
	static void Info(const char *log_type, T &log_context_source, ARGS... params) {
		Log(log_type, log_context_source, LogLevel::INFORMATIVE, params...);
	}
	template <class T, typename... ARGS>
	static void Warn(T &log_context_source, ARGS... params) {
		Log(log_context_source, LogLevel::WARNING, params...);
	}
	template <class T, typename... ARGS>
	static void Warn(const char *log_type, T &log_context_source, ARGS... params) {
		Log(log_type, log_context_source, LogLevel::WARNING, params...);
	}
	template <class T, typename... ARGS>
	static void Error(T &log_context_source, ARGS... params) {
		Log(log_context_source, LogLevel::ERROR, params...);
	}
	template <class T, typename... ARGS>
	static void Error(const char *log_type, T &log_context_source, ARGS... params) {
		Log(log_type, log_context_source, LogLevel::ERROR, params...);
	}
	template <class T, typename... ARGS>
	static void Fatal(T &log_context_source, ARGS... params) {
		Log(log_context_source, LogLevel::FATAL, params...);
	}
	template <class T, typename... ARGS>
	static void Fatal(const char *log_type, T &log_context_source, ARGS... params) {
		Log(log_type, log_context_source, LogLevel::FATAL, params...);
	}

	// TODO: implement specializations for:
	// Logger::Debug, Logger::Warning, etc. for all types of parameters, e.g.:
	//	- Logger::Debug(T source, string log_type, string message)
	//	- Logger::Debug(T source, string log_type, string message)
	//	- Logger::Debug(T source, string log_type, string format_string, ...format string params)
	//	- Logger::Debug(T source, string format_string, [](){ return ConstructExpensiveString() })

	//! TODO: implement log_type enum-ify interface:
	// //! Log message with specified log type (SLOW!)
	// virtual void Log(const char *log_type, LogLevel log_level, string &log_message)  = 0;
	// //! Log message using a specific log_type_id (FASTER)
	// virtual void Log(idx_t log_type_id, LogLevel log_level, const char *log_message)  = 0;
	// //! Get the logging type
	// virtual idx_t GetLogType(const char *log_type) = 0;

	virtual bool IsThreadSafe() = 0;
	virtual bool IsMutable() {
		return false;
	};
	virtual void UpdateConfig(LogConfig &new_config) {
		throw InternalException("Cannot update the config of this logger!");
	}

protected:
	// Pointer to manager (should be weak?)
	// Log entries are generally accumulated in loggers and then synced with the loggingManager
	// TODO: lifetime issues?
	optional_ptr<LogManager> manager;
};

// Thread-safe logger
class ThreadSafeLogger : public Logger {
public:
	explicit ThreadSafeLogger(LogConfig &config_p, LoggingContext &context_p, LogManager &manager)
	    : Logger(manager), config(config_p), context(context_p) {
		// NopLogger should be used instead
		D_ASSERT(config_p.Mode() != LogMode::DISABLED);
	}

	// Main Logger API
	bool ShouldLog(const char *log_type, LogLevel log_level) override;
	bool ShouldLog(LogLevel log_level) override;
	void WriteLog(const char *log_type, LogLevel log_level, const char *message) override;
	void WriteLog(LogLevel log_level, const char *message) override;

	void Flush() override;
	bool IsThreadSafe() override {
		return true;
	}

protected:
	const LogConfig config;
	mutex lock;
	const LoggingContext context;
};

// Non Thread-safe logger
// - will cache log entries locally
class ThreadLocalLogger : public Logger {
public:
	explicit ThreadLocalLogger(LogConfig &config_p, LoggingContext &context_p, LogManager &manager)
	    : Logger(manager), config(config_p), context(context_p) {
		// NopLogger should be used instead
		D_ASSERT(config_p.Mode() != LogMode::DISABLED);
	}

	// Main Logger API
	bool ShouldLog(const char *log_type, LogLevel log_level) override;
	bool ShouldLog(LogLevel log_level) override;
	void WriteLog(const char *log_type, LogLevel log_level, const char *message) override;
	void WriteLog(LogLevel log_level, const char *message) override;
	void Flush() override;

	bool IsThreadSafe() override {
		return false;
	}

protected:
	const LogConfig config;
	const LoggingContext context;
};

// Thread-safe Logger with mutable log settings
class MutableLogger : public Logger {
public:
	explicit MutableLogger(LogConfig &config_p, LoggingContext &context_p, LogManager &manager)
	    : Logger(manager), config(config_p), context(context_p) {
		level = config.Level();
		mode = config.Mode();
	}

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
	void UpdateConfig(LogConfig &new_config) override;

protected:
	// Atomics for lock-free log setting checks
	duckdb::atomic<LogMode> mode;
	duckdb::atomic<LogLevel> level;

	mutex lock;
	LogConfig config;
	const LoggingContext context;
};

// For when logging is disabled: NOPs everything
class NopLogger : public Logger {
public:
	explicit NopLogger() {
	}
	void Log(const char *log_type, LogLevel log_level, const char *log_message) override {
	}
	void Log(LogLevel log_level, const char *log_message) override {
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
};

// Top level class
// - Handles configuration changes
// - Creates Loggers with cached configuration
// - Main sink for logs (either by logging directly into this, or by syncing a pre-cached set of log entries)
// - Holds the logs (in case of in-memory
class LogManager : public enable_shared_from_this<LogManager> {
	friend class ThreadSafeLogger;
	friend class ThreadLocalLogger;
	friend class MutableLogger;

public:
	// Note: two step initialization because Logger needs shared pointer to log manager TODO: can we clean up?
	explicit LogManager(shared_ptr<DatabaseInstance> &db, LogConfig config = LogConfig());
	void Initialize();

	static LogManager &Get(ClientContext &context);
	unique_ptr<Logger> CreateLogger(LoggingContext &context, bool thread_safe = true, bool mutable_settings = false);

	//! The global logger can be used whe
	Logger &GlobalLogger();

	// TODO: allow modifying log settings

	unique_ptr<LogStorage> log_storage;

protected:
	// This is to be called by the Loggers only, it does not verify log_level and log_type
	void WriteLogEntry(const char *log_type, LogLevel log_level, const char *log_message,
	                   const LoggingContext &context);
	// This allows efficiently pushing a cached set of log entries into the log manager
	void FlushCachedLogEntries(DataChunk &chunk, const LoggingContext &context);

	mutex lock;
	LogConfig config;

	unique_ptr<Logger> global_logger;
};

} // namespace duckdb
