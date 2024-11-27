//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/logging/logger.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/types.hpp"
#include "duckdb/common/unordered_set.hpp"
#include "duckdb/common/mutex.hpp"
#include "duckdb/common/optional_idx.hpp"

namespace duckdb {
class TableDescription;
class DatabaseInstance;
class DataChunk;
class LogManager;
class ColumnDataCollection;

enum class LogLevel : uint8_t {
	DEBUGGING = 10,
	INFORMATIVE = 20,
	WARNING = 30,
	ERROR = 40,
	FATAL = 50,
};

struct LoggingContext {
	// TODO: potentially we might want to add stuff to identify which connection and thread the log came from
	optional_idx thread_id; // maybe: TaskScheduler::GetEstimatedCPUId ?
	optional_idx connection_id; // maybe: use the address of the connection pointer
	optional_idx query_id;
	optional_idx transaction_id;

	string default_log_type;
};

// TODO: class ColumnDataCollectionLogStorage : public LogStorage
class LogStorage {
public:
	explicit LogStorage(shared_ptr<DatabaseInstance> &db);
	~LogStorage();

	void WriteLogEntry(LogLevel level, const string& log_type, const string& log_message, const LoggingContext& context);
	void WriteLogEntries(DataChunk &chunk, const LoggingContext& context);
	void Flush();

private:
	// Cache for direct logging
	unique_ptr<DataChunk> buffer;
	idx_t buffer_size;
	idx_t max_buffer_size;

	unique_ptr<ColumnDataCollection> log_entries;
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
	const unordered_set<string> &DisabledLoggers() const{
		return disabled_loggers;
	}

	bool IsConsistent() const;

protected:
	LogConfig(LogLevel level, LogMode mode, optional_ptr<unordered_set<string>> enabled_loggers, optional_ptr<unordered_set<string>> disable_loggers);

	LogMode mode;
	LogLevel level;
	LogDestinationType output;
	unordered_set<string> enabled_loggers;
	unordered_set<string> disabled_loggers;
};

// TODO
//! Allow creating a "Logger" object from a logger string and log level that will avoid needing to check these on every log call
//! e.g:

//!     GlobalLogManager in DatabaseInstance (GlobalLogManager::Get(db)) <- or maybe this is just a log configuration?
//!     LogManager in MetaTransaction (LoggingManager used by that connection, LogManager::Get(context))
//!     Logger in ThreadContext (similar to OperatorProfiler, maybe unify with that?) - Logger::Get(thread_context)

// afhankelijk van waar je bent krijg je een ander soort logger die wel of niet thread safe is, eigen caches heeft, en eigen context heeft
// Logger::Get(thread_context)
// Logger::Get(client_context)
// Logger::Get(database_instance)

//!     Logger::Log(LogTypeId, string &log_message);
//!     Logger::Log(string &log_type, string &log_message) { Log(GetLogType(log_type), log_message); }
//!     LogTypeId Logger::GetLogType(string &log_type); <- allows for caching of LogTypeId
//!     Logger::GetLogType("HTTPFS") -> // has httpfs been registered? if so hand out the LogTypeId, otherwise the next log type

//!     Flush -> configurable after each message


//! Main interface to log stuff
//! - holds an immutable copy of the config to ensure fast determining of ShouldLog()
class Logger {
public:
	explicit Logger() : config(LogConfig()){
	}
	explicit Logger(LogConfig &config_p) : config(config_p){
	}

	virtual ~Logger() = default;

	//! Log message with default log type (empty string?)
	virtual void Log(LogLevel log_level, const string &log_type, const string &log_message) = 0;
	virtual void Flush() = 0;

	// Debug is special because it only works in debug mode
	void Debug(const string &log_type, const string &log_message) {
#ifdef DEBUG
		return Log(LogLevel::DEBUGGING, log_type, log_message);
#endif
	}

	//! TODO: implement log_type enum-ify interface
	// //! Log message with specified log type (SLOW!)
	// virtual void Log(const string &log_type, LogLevel log_level, string &log_message)  = 0;
	// //! Log message using a specific log_type_id (FASTER)
	// virtual void Log(idx_t log_type_id, LogLevel log_level, const string &log_message)  = 0;
	// //! Get the logging type
	// virtual idx_t GetLogType(const string &log_type) = 0;

	virtual bool IsThreadSafe() = 0;

protected:
	// Cached config from manager
	const LogConfig config;
	// Pointer to manager (should be weak?) TODO: set
	// Log entries are generally accumulated in loggers and then synced with the loggingManager
	shared_ptr<LogManager> manager;
};

// Thread-safe logger
class ThreadSafeLogger : public Logger {
public:
	explicit ThreadSafeLogger(LogConfig &config_p, LoggingContext &context_p) : Logger(config_p), context(context_p) {
		// NopLogger should be used instead
		D_ASSERT(config_p.Mode() != LogMode::DISABLED);
	}

	// Main Logger API
	void Log(LogLevel log_level, const string &log_type, const string &log_message) override;
	void Flush() override;
	bool IsThreadSafe() override {
		return true;
	}

protected:
	mutex lock;
	const LoggingContext context;
};

// Non Thread-safe logger
// - will cache log entries locally
class ThreadLocalLogger : public Logger {
public:
	explicit ThreadLocalLogger(LogConfig &config_p, LoggingContext &context_p) : Logger(config_p), context(context_p) {
		// NopLogger should be used instead
		D_ASSERT(config_p.Mode() != LogMode::DISABLED);
	}

	// Main Logger API
	void Log(LogLevel log_level, const string &log_type, const string &log_message) override;
	void Flush() override;

	bool IsThreadSafe() override {
		return false;
	}

protected:
	LoggingContext context;
};

// For when logging is disabled: NOPs everything
class NopLogger : public Logger {
public:
	explicit NopLogger() {
	}
	void Log(LogLevel log_level, const string &log_type, const string &log_message) override {
	}
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
class LogManager {
friend class ThreadSafeLogger;
friend class ThreadLocalLogger;
public:
	explicit LogManager(shared_ptr<DatabaseInstance> &db, LogConfig config);
	unique_ptr<Logger> CreateLogger(LoggingContext &context, bool thread_safe = true);

	// TODO: allow modifying log settings

	// static Logger& Get(ThreadContext &thread_context);
	// static Logger& Get(ClientContext &client_context);
	// static Logger& Get(DatabaseInstance &db);
	// static Logger& Get(FileOpener &opener);

protected:
	// This is to be called by the Loggers only, it does not verify log_level and log_type
	void WriteLogEntry(LogLevel log_level, const string &log_type, const string &log_message, const LoggingContext &context);
	// This allows efficiently pushing a cached set of log entries into the log manager
	void FlushCachedLogEntries(DataChunk &chunk, const LoggingContext &context);

	mutex lock;
	LogConfig config;
	unique_ptr<LogStorage> log_storage;
};

} // namespace duckdb
