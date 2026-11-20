//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/logging/log_manager.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/logging/logger.hpp"
#include "duckdb/logging/log_sink.hpp"
#include "duckdb/common/types/timestamp.hpp"
#include "duckdb/common/case_insensitive_map.hpp"

namespace duckdb {
class LogType;

//! A log sink with its type, the options the user set on it and the log entries it receives
struct LogSinkEntry {
	//! 'memory', 'stdout', 'file', or the name the sink is registered under
	string type;
	shared_ptr<LogSink> sink;
	//! The options the user set on the sink, with lowercased names
	case_insensitive_map_t<Value> config;
	//! Whether the sink receives log entries, and which ones by level and log type. Its storage is the type
	LogConfig filter;
};

//! The level and the log types to give a log sink, the ones that are not set stay as they are unless reset is set
struct LogSinkFilterUpdate {
	//! Whether the filter starts over from the default level with every log type, as enable_logging does
	bool reset = false;
	bool set_level = false;
	LogLevel level = LogConfig::DEFAULT_LOG_LEVEL;
	//! Without a level, the log types set the level to the lowest level of their entries. No log types are every type
	bool set_log_types = false;
	vector<string> log_types;
};

//! The sinks of the LogManager, see LogManager::GetConfigAndSinks
struct LogConfigAndSinks {
	//! The 'default' log sink, which enable_logging configures
	LogSinkEntry default_log_sink;
	//! The other log sinks by name, see LogManager::EnableLogSink. A disabled sink stays until the database closes
	case_insensitive_map_t<LogSinkEntry> named_log_sinks;
};

// Holds global logging state
// - Handles configuration changes
// - Creates Loggers with cached configuration
// - Holds the 'default' log sink, which enable_logging configures, and the named log sinks next to it. Each sink has
//   its own filter, the loggers use the combination of the filters of the enabled sinks
class LogManager {
	friend class ThreadSafeLogger;
	friend class ThreadLocalLogger;
	friend class MutableLogger;

public:
	// Note: two step initialization because Logger needs shared pointer to log manager TODO: can we clean up?
	explicit LogManager(DatabaseInstance &db, LogConfig config = LogConfig());
	~LogManager();
	void Initialize();

	DUCKDB_API static LogManager &Get(ClientContext &context);
	unique_ptr<Logger> CreateLogger(const LoggingContext &context, bool thread_safe = true,
	                                bool mutable_settings = false);
	//! Creates a logger from the snapshot without locking, unless the config changed since the snapshot was taken
	unique_ptr<Logger> CreateLogger(const LoggingContext &context, const LogConfigSnapshot &snapshot);
	//! Refreshes the snapshot if the config changed since it was taken, and creates a logger from it
	unique_ptr<Logger> CreateLogger(const LoggingContext &context, LogConfigSnapshot &snapshot);

	RegisteredLoggingContext RegisterLoggingContext(const LoggingContext &context);

	DUCKDB_API bool RegisterLogSink(const string &name, shared_ptr<LogSink> &sink);

	//! The global logger can be used when
	DUCKDB_API Logger &GlobalLogger();
	DUCKDB_API shared_ptr<Logger> GlobalLoggerReference();

	//! Flush everything
	DUCKDB_API void Flush();

	//! Get a shared_ptr to the default log sink (For example, to scan it)
	DUCKDB_API shared_ptr<LogSink> GetDefaultLogSink();
	//! Get the log sink with this name, enabled or not, or the default log sink if the name is empty
	DUCKDB_API shared_ptr<LogSink> GetLogSink(const string &name);
	//! Like GetLogSink, but throws if the sink cannot be scanned for this table
	DUCKDB_API shared_ptr<LogSink> GetScannableLogSink(const string &name, LoggingTargetTable table);
	//! Get the registered log sink with this name, or nullptr if there is none
	DUCKDB_API shared_ptr<LogSink> GetRegisteredLogSink(const string &name);
	//! Whether a log sink with this name exists and is enabled, the default log sink is named 'default'
	DUCKDB_API bool IsLogSinkEnabled(const string &name);
	DUCKDB_API bool CanScan(LoggingTargetTable table);

	//! Replaces the type and the filter of the default log sink
	DUCKDB_API void SetConfig(DatabaseInstance &db, const LogConfig &config);
	//! The sinks with their config and filter, to put back with RestoreConfig when a change fails
	DUCKDB_API LogConfigAndSinks GetConfigAndSinks();
	//! Puts back the sinks as GetConfigAndSinks returned them
	DUCKDB_API void RestoreConfig(LogConfigAndSinks previous);
	//! Turns the default log sink on or off, the named log sinks are turned on and off by EnableLogSink/DisableLogSink
	DUCKDB_API void SetEnableLogging(bool enable);
	//! The mode, level and log types of the default log sink
	DUCKDB_API void SetLogMode(LogMode mode);
	DUCKDB_API void SetLogLevel(LogLevel level);
	DUCKDB_API void SetEnabledLogTypes(optional_ptr<unordered_set<string>> enabled_log_types);
	DUCKDB_API void SetDisabledLogTypes(optional_ptr<unordered_set<string>> disabled_log_types);
	//! Replaces the default log sink by a sink of this type, unless it already is of this type. The default log sink
	//! keeps its filter
	DUCKDB_API void SetDefaultLogSink(DatabaseInstance &db, const string &sink_type);
	//! Warns when the sink with this name becomes a sink of this type that prints the log entries to the console next
	//! to an enabled CLI sink, which already prints them. Without a type, the sink keeps its type. Call it without
	//! holding the lock, before enabling the sink
	DUCKDB_API void WarnIfPrintedTwice(DatabaseInstance &db, const string &sink_name, const string &sink_type);
	//! Enables the log sink with this name, of this type. Without a type, an existing sink keeps its type and a new
	//! sink is named after its type. The config and the filter update the ones of an existing sink, a sink that gets
	//! another type is a new sink. The default log sink is named 'default'
	DUCKDB_API void EnableLogSink(DatabaseInstance &db, const string &sink_name, const string &sink_type = string(),
	                              const case_insensitive_map_t<Value> &sink_config = case_insensitive_map_t<Value>(),
	                              const LogSinkFilterUpdate &filter_update = LogSinkFilterUpdate());
	//! Stops sending log entries to the log sink with this name, which keeps its config, filter and log entries
	DUCKDB_API void DisableLogSink(DatabaseInstance &db, const string &sink_name);

	DUCKDB_API void UpdateDefaultLogSinkConfig(DatabaseInstance &db, case_insensitive_map_t<Value> &config_value);

	//! Enables the default log sink for these log types only, at the lowest level of their entries
	DUCKDB_API void SetEnableStructuredLoggers(vector<string> &enabled_logger_types);

	//! Truncates the log sink with this name, or the default log sink if the name is empty
	DUCKDB_API void TruncateLogSink(const string &name);

	//! Deprecated: the names from before log storages were renamed to log sinks, for the default log sink
	DUCKDB_API bool RegisterLogStorage(const string &name, shared_ptr<LogSink> &storage);
	DUCKDB_API shared_ptr<LogSink> GetLogStorage();
	DUCKDB_API void SetLogStorage(DatabaseInstance &db, const string &storage_name);
	DUCKDB_API void UpdateLogStorageConfig(DatabaseInstance &db, case_insensitive_map_t<Value> &config_value);
	DUCKDB_API void TruncateLogStorage();

	//! The config of the default log sink, its storage is the type of the sink
	DUCKDB_API LogConfig GetConfig();

	DUCKDB_API void RegisterLogType(unique_ptr<LogType> type);
	DUCKDB_API optional_ptr<const LogType> LookupLogType(const string &type);
	DUCKDB_API void RegisterDefaultLogTypes();

protected:
	RegisteredLoggingContext RegisterLoggingContextInternal(const LoggingContext &context);

	// This is to be called by the Loggers only, it does not verify log_level and log_type
	void WriteLogEntry(timestamp_t, const char *log_type, LogLevel log_level, const char *log_message,
	                   const RegisteredLoggingContext &context);
	// This allows efficiently pushing a cached set of log entries into the log manager
	void FlushCachedLogEntries(DataChunk &chunk, const RegisteredLoggingContext &context);

	//! Replaces the default log sink by a sink of this type, which gets this filter
	void SetDefaultLogSinkInternal(DatabaseInstance &db, const string &sink_type, LogConfig filter)
	    DUCKDB_REQUIRES(lock);
	//! A new built-in sink of this type, or the sink registered under this name. Throws if there is none
	shared_ptr<LogSink> CreateLogSinkInternal(DatabaseInstance &db, const string &sink_type) DUCKDB_REQUIRES(lock);
	//! The sink with this name, the default log sink for 'default', or nullptr if there is none
	optional_ptr<LogSinkEntry> GetLogSinkEntryInternal(const string &sink_name) DUCKDB_REQUIRES(lock);
	//! The name of the entry holding this sink, or an empty string if there is none
	string GetLogSinkEntryNameInternal(const LogSink &sink) DUCKDB_REQUIRES(lock);
	//! Throws if an enabled file sink other than the one with this name already writes to this path
	void VerifyLogFileUnusedInternal(const string &sink_name, const string &path) DUCKDB_REQUIRES(lock);
	//! Throws if a built-in sink of this type has no option of the config. A registered sink checks its own config
	void VerifyLogSinkConfigInternal(const string &sink_name, const string &sink_type,
	                                 const case_insensitive_map_t<Value> &sink_config) DUCKDB_REQUIRES(lock);
	//! Applies the config to the sink and remembers the options the user set
	void UpdateLogSinkConfigInternal(DatabaseInstance &db, const string &sink_name, LogSinkEntry &entry,
	                                 const case_insensitive_map_t<Value> &sink_config) DUCKDB_REQUIRES(lock);
	//! Applies the level and the log types to the filter of a sink
	void UpdateLogSinkFilterInternal(LogConfig &filter, const LogSinkFilterUpdate &filter_update) DUCKDB_REQUIRES(lock);
	shared_ptr<LogSink> GetLogSinkInternal(const string &name) DUCKDB_REQUIRES(lock);
	//! The error for a name that is not the name of a sink
	InvalidInputException LogSinkDoesNotExistInternal(const string &name) DUCKDB_REQUIRES(lock);

	optional_ptr<const LogType> LookupLogTypeInternal(const string &type) DUCKDB_REQUIRES(lock);

	//! The only way to change the config of the loggers: combines the filters of the enabled sinks, and publishes the
	//! result to the global logger and to newly created loggers. Call it after any change to the sinks
	void UpdateLoggerConfigInternal() DUCKDB_REQUIRES(lock);
	unique_ptr<Logger> CreateLoggerInternal(const LoggingContext &context,
	                                        const shared_ptr<const LogConfig> &logger_config);

protected:
	annotated_mutex lock;
	//! The combined filter of the enabled sinks, shared by the created loggers; replaced only by
	//! UpdateLoggerConfigInternal
	shared_ptr<const LogConfig> config DUCKDB_GUARDED_BY(lock);
	//! Incremented whenever config is replaced, invalidating the snapshots of it
	PaddedAtomic<idx_t> config_version {0};

	shared_ptr<Logger> global_logger;
	//! The log sink named 'default', which enable_logging configures
	LogSinkEntry default_log_sink DUCKDB_GUARDED_BY(lock);
	//! The log sinks next to the default log sink by name, a registered sink has one name only
	case_insensitive_map_t<LogSinkEntry> named_log_sinks DUCKDB_GUARDED_BY(lock);
	DatabaseInstance &db_instance;

	PaddedAtomic<idx_t> next_registered_logging_context_index {0};

	// The LogSinks registered with RegisterLogSink, by extensions for example
	case_insensitive_map_t<shared_ptr<LogSink>> registered_log_sinks DUCKDB_GUARDED_BY(lock);
	case_insensitive_map_t<unique_ptr<LogType>> registered_log_types DUCKDB_GUARDED_BY(lock);
};

} // namespace duckdb
