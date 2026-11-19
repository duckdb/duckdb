#include "duckdb/logging/log_type.hpp"
#include "duckdb/logging/log_manager.hpp"
#include "duckdb/logging/log_sink.hpp"
#include "duckdb/logging/file_system_logger.hpp"
#include "duckdb/main/database.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/main/client_data.hpp"
#include "duckdb/main/settings.hpp"
#include "duckdb/common/local_file_system.hpp"

namespace duckdb {

struct DeprecatedLogSinkName {
	const char *old_name;
	const char *new_name;
};

static const DeprecatedLogSinkName DEPRECATED_LOG_SINK_NAMES[] = {{"shell_log_storage", "shell_log_sink"}};

//! The sink the CLI registers to print the log entries to the console
static constexpr const char *SHELL_LOG_SINK_NAME = "shell_log_sink";

//! Maps a deprecated sink name to its new name and warns. Call it without holding the LogManager lock
static string ResolveDeprecatedLogSinkName(DatabaseInstance &db, const string &sink_name) {
	for (auto &entry : DEPRECATED_LOG_SINK_NAMES) {
		if (StringUtil::CIEquals(sink_name, entry.old_name)) {
			DUCKDB_LOG_WARNING(db, "The log sink '%s' has been renamed to '%s', the old name is deprecated",
			                   entry.old_name, entry.new_name);
			return entry.new_name;
		}
	}
	return sink_name;
}

unique_ptr<Logger> LogManager::CreateLogger(const LoggingContext &context, bool thread_safe, bool mutable_settings) {
	annotated_unique_lock<annotated_mutex> lck(lock);
	if (mutable_settings) {
		return make_uniq<MutableLogger>(*config, RegisterLoggingContextInternal(context), *this);
	}
	if (!thread_safe) {
		// TODO: implement ThreadLocalLogger and return it here
	}
	return CreateLoggerInternal(context, config);
}

unique_ptr<Logger> LogManager::CreateLogger(const LoggingContext &context, const LogConfigSnapshot &snapshot) {
	if (snapshot.config && snapshot.version == config_version.load()) {
		return CreateLoggerInternal(context, snapshot.config);
	}
	annotated_unique_lock<annotated_mutex> lck(lock);
	return CreateLoggerInternal(context, config);
}

unique_ptr<Logger> LogManager::CreateLogger(const LoggingContext &context, LogConfigSnapshot &snapshot) {
	if (!snapshot.config || snapshot.version != config_version.load()) {
		annotated_unique_lock<annotated_mutex> lck(lock);
		snapshot.config = config;
		snapshot.version = config_version.load();
	}
	return CreateLoggerInternal(context, snapshot.config);
}

unique_ptr<Logger> LogManager::CreateLoggerInternal(const LoggingContext &context,
                                                    const shared_ptr<const LogConfig> &logger_config) {
	if (!logger_config->enabled) {
		return make_uniq<NopLogger>(*this);
	}
	return make_uniq<ThreadSafeLogger>(logger_config, RegisterLoggingContextInternal(context), *this);
}

RegisteredLoggingContext LogManager::RegisterLoggingContext(const LoggingContext &context) {
	return RegisterLoggingContextInternal(context);
}

bool LogManager::RegisterLogSink(const string &name, shared_ptr<LogSink> &sink) {
	annotated_unique_lock<annotated_mutex> lck(lock);
	// the built-in sinks are created by the LogManager, they cannot be replaced
	auto name_to_lower = StringUtil::Lower(name);
	if (LogConfig::IsBuiltInLogSink(name_to_lower) || name_to_lower == LogConfig::DEFAULT_LOG_SINK_NAME ||
	    registered_log_sinks.find(name) != registered_log_sinks.end()) {
		return false;
	}
	registered_log_sinks.insert({name, std::move(sink)});
	return true;
}

Logger &LogManager::GlobalLogger() {
	return *global_logger;
}

shared_ptr<Logger> LogManager::GlobalLoggerReference() {
	return global_logger;
}

void LogManager::Flush() {
	annotated_unique_lock<annotated_mutex> lck(lock);
	default_log_sink.sink->FlushAll();
	for (auto &entry : named_log_sinks) {
		entry.second.sink->FlushAll();
	}
}

shared_ptr<LogSink> LogManager::GetDefaultLogSink() {
	annotated_unique_lock<annotated_mutex> lck(lock);
	return default_log_sink.sink;
}

shared_ptr<LogSink> LogManager::GetLogSink(const string &name) {
	annotated_unique_lock<annotated_mutex> lck(lock);
	return GetLogSinkInternal(name);
}

optional_ptr<EnabledLogSink> LogManager::GetEnabledLogSinkInternal(const string &sink_name) {
	if (StringUtil::CIEquals(sink_name, LogConfig::DEFAULT_LOG_SINK_NAME)) {
		return default_log_sink;
	}
	auto entry = named_log_sinks.find(sink_name);
	if (entry == named_log_sinks.end()) {
		return nullptr;
	}
	return entry->second;
}

string LogManager::GetEnabledLogSinkNameInternal(const LogSink &sink) {
	if (default_log_sink.sink.get() == &sink) {
		return LogConfig::DEFAULT_LOG_SINK_NAME;
	}
	for (auto &entry : named_log_sinks) {
		if (entry.second.sink.get() == &sink) {
			return entry.first;
		}
	}
	return string();
}

shared_ptr<LogSink> LogManager::GetLogSinkInternal(const string &name) {
	if (name.empty()) {
		return default_log_sink.sink;
	}
	auto enabled_sink = GetEnabledLogSinkInternal(name);
	if (enabled_sink) {
		return enabled_sink->sink;
	}
	throw LogSinkNotEnabledInternal(name);
}

InvalidInputException LogManager::LogSinkNotEnabledInternal(const string &name) {
	// a type of sink can be enabled again, a name of your own only exists while its sink is enabled
	auto name_to_lower = StringUtil::Lower(name);
	if (!LogConfig::IsBuiltInLogSink(name_to_lower) && registered_log_sinks.find(name) == registered_log_sinks.end()) {
		return InvalidInputException("Log sink '%s' does not exist", name);
	}
	// the name of a type does not refer to the default log sink of that type
	if (default_log_sink.type == name_to_lower) {
		return InvalidInputException("Log sink '%s' is not enabled. The default log sink is a '%s' log sink, its name "
		                             "is 'default'",
		                             name, name_to_lower);
	}
	return InvalidInputException("Log sink '%s' is not enabled", name);
}

shared_ptr<LogSink> LogManager::GetScannableLogSink(const string &name, LoggingTargetTable table) {
	annotated_unique_lock<annotated_mutex> lck(lock);
	auto sink = GetLogSinkInternal(name);
	if (!sink->CanScan(table)) {
		if (name.empty()) {
			throw InvalidConfigurationException(
			    "Log sink '%s' does not support this query. Select a queryable sink, such as 'memory' or 'file', "
			    "before generating logs.",
			    config->storage);
		}
		throw InvalidConfigurationException("Log sink '%s' does not support this query", name);
	}
	return sink;
}

shared_ptr<LogSink> LogManager::GetRegisteredLogSink(const string &name) {
	annotated_unique_lock<annotated_mutex> lck(lock);
	auto entry = registered_log_sinks.find(name);
	if (entry == registered_log_sinks.end()) {
		return nullptr;
	}
	return entry->second;
}

bool LogManager::IsLogSinkEnabled(const string &name) {
	annotated_unique_lock<annotated_mutex> lck(lock);
	return GetEnabledLogSinkInternal(name).get() != nullptr;
}

bool LogManager::CanScan(LoggingTargetTable table) {
	annotated_unique_lock<annotated_mutex> lck(lock);
	return default_log_sink.sink->CanScan(table);
}

LogManager::LogManager(DatabaseInstance &db, LogConfig config_p)
    : config(make_shared_ptr<const LogConfig>(std::move(config_p))), db_instance(db) {
	default_log_sink = EnabledLogSink {LogConfig::IN_MEMORY_STORAGE_NAME, make_shared_ptr<InMemoryLogSink>(db),
	                                   case_insensitive_map_t<Value>()};
	default_log_sink_enabled = config->enabled;
}

LogManager::~LogManager() {
}

void LogManager::Initialize() {
	LoggingContext context(LogContextScope::DATABASE);
	global_logger = CreateLogger(context, true, true);

	RegisterDefaultLogTypes();
}

LogManager &LogManager::Get(ClientContext &context) {
	return context.db->GetLogManager();
}

RegisteredLoggingContext LogManager::RegisterLoggingContextInternal(const LoggingContext &context) {
	auto context_id = next_registered_logging_context_index++;
	if (context_id >= NumericLimits<idx_t>::Maximum() - 1) {
		throw InternalException("Ran out of available log context ids.");
	}
	return {context_id, context};
}

void LogManager::WriteLogEntry(timestamp_t timestamp, const char *log_type, LogLevel log_level, const char *log_message,
                               const RegisteredLoggingContext &context) {
	if (log_level == LogLevel::LOG_WARNING && Settings::Get<WarningsAsErrorsSetting>(db_instance)) {
		throw InvalidInputException(log_message);
	} else {
		annotated_unique_lock<annotated_mutex> lck(lock);
		if (default_log_sink_enabled) {
			default_log_sink.sink->WriteLogEntry(timestamp, log_level, log_type, log_message, context);
		}
		for (auto &entry : named_log_sinks) {
			entry.second.sink->WriteLogEntry(timestamp, log_level, log_type, log_message, context);
		}
	}
}

void LogManager::FlushCachedLogEntries(DataChunk &chunk, const RegisteredLoggingContext &context) {
	throw NotImplementedException("FlushCachedLogEntries");
}

void LogManager::SetConfig(DatabaseInstance &db, const LogConfig &config_p) {
	auto sink_type = ResolveDeprecatedLogSinkName(db, config_p.storage);

	annotated_unique_lock<annotated_mutex> lck(lock);

	// We need extra handling for switching sink
	auto new_config = config_p;
	SetDefaultLogSinkInternal(db, new_config, sink_type);
	SetConfigInternal(std::move(new_config));
}

LogConfigAndSinks LogManager::GetConfigAndSinks() {
	annotated_unique_lock<annotated_mutex> lck(lock);
	return LogConfigAndSinks {*config, default_log_sink, named_log_sinks, default_log_sink_enabled};
}

void LogManager::RestoreConfig(LogConfigAndSinks previous) {
	annotated_unique_lock<annotated_mutex> lck(lock);
	default_log_sink = std::move(previous.default_log_sink);
	named_log_sinks = std::move(previous.named_log_sinks);
	default_log_sink_enabled = previous.default_log_sink_enabled;
	SetConfigInternal(std::move(previous.config));
}

void LogManager::SetEnableLogging(bool enable) {
	annotated_unique_lock<annotated_mutex> lck(lock);
	default_log_sink_enabled = enable;
	auto new_config = *config;
	new_config.enabled = enable;
	SetConfigInternal(std::move(new_config));
}

void LogManager::SetLogMode(LogMode mode) {
	annotated_unique_lock<annotated_mutex> lck(lock);
	auto new_config = *config;
	new_config.mode = mode;
	SetConfigInternal(std::move(new_config));
}

void LogManager::SetLogLevel(LogLevel level) {
	annotated_unique_lock<annotated_mutex> lck(lock);
	auto new_config = *config;
	new_config.level = level;
	SetConfigInternal(std::move(new_config));
}

void LogManager::SetEnabledLogTypes(optional_ptr<unordered_set<string>> enabled_log_types) {
	annotated_unique_lock<annotated_mutex> lck(lock);
	auto new_config = *config;
	if (enabled_log_types) {
		new_config.enabled_log_types = *enabled_log_types;
	} else {
		new_config.enabled_log_types = {};
	}
	SetConfigInternal(std::move(new_config));
}

void LogManager::SetDisabledLogTypes(optional_ptr<unordered_set<string>> disabled_log_types) {
	annotated_unique_lock<annotated_mutex> lck(lock);
	auto new_config = *config;
	if (disabled_log_types) {
		new_config.disabled_log_types = *disabled_log_types;
	} else {
		new_config.disabled_log_types = {};
	}
	SetConfigInternal(std::move(new_config));
}

void LogManager::WarnIfPrintedTwice(DatabaseInstance &db, const string &new_sink_type) {
	if (!StringUtil::CIEquals(new_sink_type, LogConfig::STDOUT_STORAGE_NAME)) {
		return;
	}
	bool shell_sink_is_named = false;
	{
		annotated_unique_lock<annotated_mutex> lck(lock);
		for (auto &entry : named_log_sinks) {
			shell_sink_is_named |= StringUtil::CIEquals(entry.second.type, SHELL_LOG_SINK_NAME);
		}
	}
	if (shell_sink_is_named) {
		DUCKDB_LOG_WARNING(db, "The 'stdout' log sink and 'shell_log_sink' both print the log entries to the console.\n"
		                       "To print them once, run CALL disable_log_sink('shell_log_sink').");
	}
}

void LogManager::SetDefaultLogSink(DatabaseInstance &db, const string &requested_sink_type) {
	auto sink_type = StringUtil::Lower(ResolveDeprecatedLogSinkName(db, requested_sink_type));
	if (GetConfig().storage != sink_type) {
		WarnIfPrintedTwice(db, sink_type);
	}

	annotated_unique_lock<annotated_mutex> lck(lock);
	// 'SET logging_sink' cannot supply the path that file sink requires, so reject the switch
	// here (active sink preserved) and point users at enable_logging instead of installing a
	// path-less sink that throws on every later flush.
	if (sink_type == LogConfig::FILE_STORAGE_NAME && config->storage != sink_type) {
		throw InvalidConfigurationException(
		    "Cannot select 'file' log sink via 'SET logging_sink' because it requires a path. "
		    "Use CALL enable_logging(type := 'file', config := {path: '...'}) instead.");
	}
	auto new_config = *config;
	SetDefaultLogSinkInternal(db, new_config, sink_type);
	SetConfigInternal(std::move(new_config));
}

void LogManager::SetDefaultLogSinkInternal(DatabaseInstance &db, LogConfig &new_config, const string &sink_type) {
	auto sink_type_to_lower = StringUtil::Lower(sink_type);

	// keep the lowercased type, so that it is recognized when set again
	new_config.storage = sink_type_to_lower;
	if (config->storage == sink_type_to_lower) {
		return;
	}

	auto new_sink = CreateLogSinkInternal(db, sink_type_to_lower);
	// a registered sink exists once: when it is enabled under a name, it moves to the default log sink
	auto enabled_name = GetEnabledLogSinkNameInternal(*new_sink);
	if (!enabled_name.empty()) {
		named_log_sinks.erase(enabled_name);
	}

	// Flush the old sink, we are going to replace it.
	default_log_sink.sink->FlushAll();
	default_log_sink = EnabledLogSink {sink_type_to_lower, std::move(new_sink), case_insensitive_map_t<Value>()};
}

shared_ptr<LogSink> LogManager::CreateLogSinkInternal(DatabaseInstance &db, const string &sink_type) {
	auto sink_type_to_lower = StringUtil::Lower(sink_type);
	if (sink_type_to_lower == LogConfig::IN_MEMORY_STORAGE_NAME) {
		return make_shared_ptr<InMemoryLogSink>(db);
	}
	if (sink_type_to_lower == LogConfig::STDOUT_STORAGE_NAME) {
		return make_shared_ptr<StdOutLogSink>(db);
	}
	if (sink_type_to_lower == LogConfig::FILE_STORAGE_NAME) {
		auto &fs = FileSystem::GetFileSystem(db);
		if (fs.SubSystemIsDisabled(LocalFileSystem().GetName())) {
			throw InvalidConfigurationException("Can not enable file logging with the LocalFileSystem disabled");
		}
		return make_shared_ptr<FileLogSink>(db);
	}
	auto entry = registered_log_sinks.find(sink_type_to_lower);
	if (entry == registered_log_sinks.end()) {
		throw InvalidInputException("Log sink '%s' is not yet registered", sink_type);
	}
	return entry->second;
}

//! Remembers the options the user set on a sink, to list them in duckdb_log_sinks
static void RememberSinkConfig(EnabledLogSink &sink, const case_insensitive_map_t<Value> &sink_config) {
	for (auto &entry : sink_config) {
		sink.config[StringUtil::Lower(entry.first)] = entry.second;
	}
}

//! The path in a sink config, or an empty string if it has none
static string GetSinkConfigPath(const case_insensitive_map_t<Value> &sink_config) {
	auto path_entry = sink_config.find("path");
	if (path_entry == sink_config.end() || path_entry->second.IsNull()) {
		return string();
	}
	return path_entry->second.ToString();
}

void LogManager::VerifyLogFileUnusedInternal(const string &sink_name,
                                             const case_insensitive_map_t<Value> &sink_config) {
	auto path = GetSinkConfigPath(sink_config);
	if (path.empty()) {
		return;
	}
	auto verify_sink = [&](const string &other_name, EnabledLogSink &other) {
		if (StringUtil::CIEquals(other_name, sink_name) || other.type != LogConfig::FILE_STORAGE_NAME) {
			return;
		}
		if (GetSinkConfigPath(other.config) == path) {
			throw InvalidInputException("Log sink '%s' already writes to '%s', choose another path", other_name, path);
		}
	};
	verify_sink(LogConfig::DEFAULT_LOG_SINK_NAME, default_log_sink);
	for (auto &entry : named_log_sinks) {
		verify_sink(entry.first, entry.second);
	}
}

//! Throws unless a sink of this type can be enabled under this name. 'default' is the sink of enable_logging, and the
//! name of a type of sink is reserved for that type
static void VerifyLogSinkName(const string &sink_name, const string &sink_type,
                              const case_insensitive_map_t<shared_ptr<LogSink>> &registered_log_sinks) {
	if (sink_name == LogConfig::DEFAULT_LOG_SINK_NAME) {
		throw InvalidInputException("The 'default' log sink is the one enable_logging configures, change it with "
		                            "CALL enable_logging(type := '...')");
	}
	bool is_type =
	    LogConfig::IsBuiltInLogSink(sink_type) || registered_log_sinks.find(sink_type) != registered_log_sinks.end();
	if (!is_type) {
		if (sink_name == sink_type) {
			throw InvalidInputException("Log sink '%s' is not yet registered. To enable a log sink under a name of "
			                            "your own, give its type, e.g. CALL enable_log_sink('%s', type='memory')",
			                            sink_name, sink_name);
		}
		throw InvalidInputException("Unknown log sink type '%s', use 'memory', 'stdout', 'file' or the name of a "
		                            "registered log sink",
		                            sink_type);
	}
	if (sink_name != sink_type && (LogConfig::IsBuiltInLogSink(sink_name) ||
	                               registered_log_sinks.find(sink_name) != registered_log_sinks.end())) {
		throw InvalidInputException("The log sink name '%s' is reserved for the '%s' log sink, choose another name",
		                            sink_name, sink_name);
	}
}

void LogManager::EnableLogSink(DatabaseInstance &db, const string &requested_sink_name,
                               const string &requested_sink_type, const case_insensitive_map_t<Value> &sink_config) {
	auto sink_name = StringUtil::Lower(ResolveDeprecatedLogSinkName(db, requested_sink_name));
	auto sink_type = requested_sink_type.empty()
	                     ? string()
	                     : StringUtil::Lower(ResolveDeprecatedLogSinkName(db, requested_sink_type));
	auto config_copy = sink_config;

	annotated_unique_lock<annotated_mutex> lck(lock);
	auto enabled_sink = GetEnabledLogSinkInternal(sink_name);
	if (sink_type.empty()) {
		// an enabled sink keeps its type, a new sink without a type is named after its type
		sink_type = enabled_sink ? enabled_sink->type : sink_name;
	}
	VerifyLogSinkName(sink_name, sink_type, registered_log_sinks);
	if (enabled_sink && sink_type != enabled_sink->type) {
		throw InvalidInputException("Log sink '%s' is a '%s' log sink, disable it before enabling a '%s' log sink "
		                            "with this name",
		                            sink_name, enabled_sink->type, sink_type);
	}
	if (!enabled_sink && sink_type == LogConfig::FILE_STORAGE_NAME && GetSinkConfigPath(config_copy).empty()) {
		auto type_parameter = sink_name == sink_type ? string() : StringUtil::Format(", type='%s'", sink_type);
		throw InvalidInputException(
		    "The 'file' log sink requires a path, e.g. CALL enable_log_sink('%s'%s, config={path: 'log.csv'})",
		    sink_name, type_parameter);
	}
	if (sink_type == LogConfig::FILE_STORAGE_NAME) {
		VerifyLogFileUnusedInternal(sink_name, config_copy);
	}

	if (enabled_sink) {
		// the sink already receives the log entries, only update its config
		if (!config_copy.empty()) {
			enabled_sink->sink->UpdateConfig(db, config_copy);
			RememberSinkConfig(*enabled_sink, sink_config);
		}
	} else {
		auto sink = CreateLogSinkInternal(db, sink_type);
		// a registered sink exists once, so it can be enabled under one name only
		auto enabled_name = GetEnabledLogSinkNameInternal(*sink);
		if (!enabled_name.empty()) {
			throw InvalidInputException("Log sink '%s' is already enabled as '%s'", sink_type, enabled_name);
		}
		if (!config_copy.empty()) {
			sink->UpdateConfig(db, config_copy);
		}
		EnabledLogSink named_sink {sink_type, std::move(sink), case_insensitive_map_t<Value>()};
		RememberSinkConfig(named_sink, sink_config);
		named_log_sinks[sink_name] = std::move(named_sink);
	}

	if (!config->enabled) {
		auto new_config = *config;
		new_config.enabled = true;
		SetConfigInternal(std::move(new_config));
	}
}

void LogManager::DisableLogSink(DatabaseInstance &db, const string &requested_sink_name) {
	auto sink_name = StringUtil::Lower(ResolveDeprecatedLogSinkName(db, requested_sink_name));

	annotated_unique_lock<annotated_mutex> lck(lock);
	if (sink_name == LogConfig::DEFAULT_LOG_SINK_NAME) {
		throw InvalidInputException(
		    "The 'default' log sink cannot be disabled, stop logging with "
		    "CALL disable_logging() or change the sink with CALL enable_logging(type := '...')");
	}
	auto entry = named_log_sinks.find(sink_name);
	if (entry == named_log_sinks.end()) {
		throw LogSinkNotEnabledInternal(requested_sink_name);
	}
	entry->second.sink->FlushAll();
	named_log_sinks.erase(entry);

	// nothing receives the log entries anymore, so stop creating them
	if (named_log_sinks.empty() && !default_log_sink_enabled && config->enabled) {
		auto new_config = *config;
		new_config.enabled = false;
		SetConfigInternal(std::move(new_config));
	}
}

void LogManager::UpdateDefaultLogSinkConfig(DatabaseInstance &db, case_insensitive_map_t<Value> &config_value) {
	annotated_unique_lock<annotated_mutex> lck(lock);
	if (default_log_sink.type == LogConfig::FILE_STORAGE_NAME) {
		VerifyLogFileUnusedInternal(LogConfig::DEFAULT_LOG_SINK_NAME, config_value);
	}
	auto user_config = config_value;
	default_log_sink.sink->UpdateConfig(db, config_value);
	RememberSinkConfig(default_log_sink, user_config);
}

void LogManager::SetEnableStructuredLoggers(vector<string> &enabled_logger_types) {
	annotated_unique_lock<annotated_mutex> lck(lock);

	LogConfig new_config = *config;
	new_config.enabled_log_types.clear();

	LogLevel min_log_level = LogLevel::LOG_FATAL;

	for (const auto &enabled_logger_type : enabled_logger_types) {
		auto lookup = LookupLogTypeInternal(enabled_logger_type);
		if (!lookup) {
			throw InvalidInputException("Unknown log type: '%s'", enabled_logger_type);
		}

		new_config.enabled_log_types.insert(lookup->name);

		min_log_level = MinValue(min_log_level, lookup->level);
	}

	new_config.level = min_log_level;
	new_config.mode = LogMode::ENABLE_SELECTED;
	new_config.enabled = true;
	default_log_sink_enabled = true;

	SetConfigInternal(std::move(new_config));
}

void LogManager::TruncateLogSink(const string &name) {
	annotated_unique_lock<annotated_mutex> lck(lock);
	GetLogSinkInternal(name)->Truncate();
}

LogConfig LogManager::GetConfig() {
	annotated_unique_lock<annotated_mutex> lck(lock);
	return *config;
}

optional_ptr<const LogType> LogManager::LookupLogType(const string &type) {
	annotated_unique_lock<annotated_mutex> lck(lock);
	return LookupLogTypeInternal(type);
}

DUCKDB_API void RegisterDefaultLogTypes() {
}

optional_ptr<const LogType> LogManager::LookupLogTypeInternal(const string &type) {
	auto lookup = registered_log_types.find(type);
	if (lookup != registered_log_types.end()) {
		return *lookup->second;
	}
	return nullptr;
}

void LogManager::SetConfigInternal(LogConfig new_config) {
	config = make_shared_ptr<const LogConfig>(std::move(new_config));
	config_version++;
	global_logger->UpdateConfig(*config);
}

void LogManager::RegisterLogType(unique_ptr<LogType> type) {
	annotated_unique_lock<annotated_mutex> lck(lock);

	auto lookup = registered_log_types.find(type->name);
	if (lookup != registered_log_types.end()) {
		throw InvalidInputException("Registered log writer '%s' already exists", type->name);
	}

	registered_log_types[type->name] = std::move(type);
}

void LogManager::RegisterDefaultLogTypes() {
	RegisterLogType(make_uniq<DefaultLogType>());
	RegisterLogType(make_uniq<FileSystemLogType>());
	RegisterLogType(make_uniq<HTTPLogType>());
	RegisterLogType(make_uniq<QueryLogType>());
	RegisterLogType(make_uniq<PhysicalOperatorLogType>());
	RegisterLogType(make_uniq<MetricsLogType>());
	RegisterLogType(make_uniq<AdaptiveFilterLogType>());
	RegisterLogType(make_uniq<ParquetPrefetchLogType>());
	RegisterLogType(make_uniq<AsyncTaskScheduleLogType>());
	RegisterLogType(make_uniq<ReadAheadBudgetLogType>());
	RegisterLogType(make_uniq<ExternalResourceLogType>());
	RegisterLogType(make_uniq<ProgressVerificationLogType>());
}

} // namespace duckdb
