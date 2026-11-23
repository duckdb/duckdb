#include "duckdb/logging/log_type.hpp"
#include "duckdb/logging/log_manager.hpp"
#include "duckdb/logging/log_sink.hpp"
#include "duckdb/logging/file_system_logger.hpp"
#include "duckdb/main/database.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/main/client_data.hpp"
#include "duckdb/main/settings.hpp"
#include "duckdb/common/algorithm.hpp"
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

optional_ptr<LogSinkEntry> LogManager::GetLogSinkEntryInternal(const string &sink_name) {
	if (StringUtil::CIEquals(sink_name, LogConfig::DEFAULT_LOG_SINK_NAME)) {
		return default_log_sink;
	}
	auto entry = named_log_sinks.find(sink_name);
	if (entry == named_log_sinks.end()) {
		return nullptr;
	}
	return entry->second;
}

string LogManager::GetLogSinkEntryNameInternal(const LogSink &sink) {
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
	auto entry = GetLogSinkEntryInternal(name);
	if (entry) {
		return entry->sink;
	}
	throw LogSinkDoesNotExistInternal(name);
}

InvalidInputException LogManager::LogSinkDoesNotExistInternal(const string &name) {
	// the name of a type does not refer to the default log sink of that type
	auto name_to_lower = StringUtil::Lower(name);
	if (default_log_sink.type == name_to_lower) {
		return InvalidInputException("Log sink '%s' does not exist. The default log sink is a '%s' log sink, its name "
		                             "is 'default'",
		                             name, name_to_lower);
	}
	return InvalidInputException("Log sink '%s' does not exist", name);
}

shared_ptr<LogSink> LogManager::GetScannableLogSink(const string &name, LoggingTargetTable table) {
	annotated_unique_lock<annotated_mutex> lck(lock);
	auto sink = GetLogSinkInternal(name);
	if (!sink->CanScan(table)) {
		auto sink_name = name.empty() ? string(LogConfig::DEFAULT_LOG_SINK_NAME) : StringUtil::Lower(name);
		throw InvalidConfigurationException(
		    "Log sink '%s' is a '%s' log sink, which cannot be read. To read log entries, store them in a log sink of "
		    "their own, e.g. CALL enable_log_sink('my_logs', type := 'memory'), then read them with FROM "
		    "duckdb_logs('my_logs')",
		    sink_name, GetLogSinkEntryInternal(sink_name)->type);
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
	auto entry = GetLogSinkEntryInternal(name);
	return entry && entry->filter.enabled;
}

bool LogManager::CanScan(LoggingTargetTable table) {
	annotated_unique_lock<annotated_mutex> lck(lock);
	return default_log_sink.sink->CanScan(table);
}

//! The filter of the loggers: a log entry that an enabled sink accepts passes it, and each sink filters it again
static LogConfig CombineLogSinkFilters(const LogSinkEntry &default_log_sink,
                                       const case_insensitive_map_t<LogSinkEntry> &named_log_sinks) {
	vector<reference<const LogConfig>> filters;
	if (default_log_sink.filter.enabled) {
		filters.push_back(default_log_sink.filter);
	}
	for (auto &entry : named_log_sinks) {
		if (entry.second.filter.enabled) {
			filters.push_back(entry.second.filter);
		}
	}
	if (filters.size() == 1) {
		auto result = filters[0].get();
		result.storage = default_log_sink.type;
		return result;
	}
	LogConfig result;
	result.storage = default_log_sink.type;
	if (filters.empty()) {
		return result;
	}
	result.enabled = true;
	result.level = LogLevel::LOG_FATAL;
	bool every_type = false;
	bool selects_types = true;
	for (auto &filter : filters) {
		result.level = MinValue(result.level, filter.get().level);
		every_type |= filter.get().mode == LogMode::LEVEL_ONLY;
		selects_types &= filter.get().mode == LogMode::ENABLE_SELECTED;
	}
	if (every_type) {
		return result;
	}
	if (selects_types) {
		result.mode = LogMode::ENABLE_SELECTED;
		for (auto &filter : filters) {
			auto &log_types = filter.get().enabled_log_types;
			result.enabled_log_types.insert(log_types.begin(), log_types.end());
		}
		return result;
	}
	// a log type is left out when every sink that leaves out log types leaves it out, and no sink selects it
	bool first = true;
	for (auto &filter : filters) {
		if (filter.get().mode != LogMode::DISABLE_SELECTED) {
			continue;
		}
		if (first) {
			result.disabled_log_types = filter.get().disabled_log_types;
			first = false;
			continue;
		}
		for (auto it = result.disabled_log_types.begin(); it != result.disabled_log_types.end();) {
			if (filter.get().disabled_log_types.count(*it) == 0) {
				it = result.disabled_log_types.erase(it);
			} else {
				it++;
			}
		}
	}
	for (auto &filter : filters) {
		if (filter.get().mode == LogMode::ENABLE_SELECTED) {
			for (auto &log_type : filter.get().enabled_log_types) {
				result.disabled_log_types.erase(log_type);
			}
		}
	}
	result.mode = result.disabled_log_types.empty() ? LogMode::LEVEL_ONLY : LogMode::DISABLE_SELECTED;
	return result;
}

LogManager::LogManager(DatabaseInstance &db, LogConfig config_p) : db_instance(db) {
	config_p.storage = LogConfig::IN_MEMORY_STORAGE_NAME;
	default_log_sink = LogSinkEntry {LogConfig::IN_MEMORY_STORAGE_NAME, make_shared_ptr<InMemoryLogSink>(db),
	                                 case_insensitive_map_t<Value>(), std::move(config_p)};
	config = make_shared_ptr<const LogConfig>(CombineLogSinkFilters(default_log_sink, named_log_sinks));
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
		if (default_log_sink.filter.Accepts(log_type, log_level)) {
			default_log_sink.sink->WriteLogEntry(timestamp, log_level, log_type, log_message, context);
		}
		for (auto &entry : named_log_sinks) {
			if (entry.second.filter.Accepts(log_type, log_level)) {
				entry.second.sink->WriteLogEntry(timestamp, log_level, log_type, log_message, context);
			}
		}
	}
}

void LogManager::FlushCachedLogEntries(DataChunk &chunk, const RegisteredLoggingContext &context) {
	throw NotImplementedException("FlushCachedLogEntries");
}

void LogManager::SetConfig(DatabaseInstance &db, const LogConfig &config_p) {
	auto sink_type = ResolveDeprecatedLogSinkName(db, config_p.storage);

	annotated_unique_lock<annotated_mutex> lck(lock);
	SetDefaultLogSinkInternal(db, sink_type, config_p);
	UpdateLoggerConfigInternal();
}

LogConfigAndSinks LogManager::GetConfigAndSinks() {
	annotated_unique_lock<annotated_mutex> lck(lock);
	return LogConfigAndSinks {default_log_sink, named_log_sinks};
}

void LogManager::RestoreConfig(LogConfigAndSinks previous) {
	annotated_unique_lock<annotated_mutex> lck(lock);
	default_log_sink = std::move(previous.default_log_sink);
	named_log_sinks = std::move(previous.named_log_sinks);
	UpdateLoggerConfigInternal();
}

void LogManager::SetEnableLogging(bool enable) {
	annotated_unique_lock<annotated_mutex> lck(lock);
	default_log_sink.filter.enabled = enable;
	UpdateLoggerConfigInternal();
}

void LogManager::SetLogMode(LogMode mode) {
	annotated_unique_lock<annotated_mutex> lck(lock);
	default_log_sink.filter.mode = mode;
	UpdateLoggerConfigInternal();
}

void LogManager::SetLogLevel(LogLevel level) {
	annotated_unique_lock<annotated_mutex> lck(lock);
	default_log_sink.filter.level = level;
	UpdateLoggerConfigInternal();
}

void LogManager::SetEnabledLogTypes(optional_ptr<unordered_set<string>> enabled_log_types) {
	annotated_unique_lock<annotated_mutex> lck(lock);
	if (enabled_log_types) {
		default_log_sink.filter.enabled_log_types = *enabled_log_types;
	} else {
		default_log_sink.filter.enabled_log_types = {};
	}
	UpdateLoggerConfigInternal();
}

void LogManager::SetDisabledLogTypes(optional_ptr<unordered_set<string>> disabled_log_types) {
	annotated_unique_lock<annotated_mutex> lck(lock);
	if (disabled_log_types) {
		default_log_sink.filter.disabled_log_types = *disabled_log_types;
	} else {
		default_log_sink.filter.disabled_log_types = {};
	}
	UpdateLoggerConfigInternal();
}

void LogManager::WarnIfPrintedTwice(DatabaseInstance &db, const string &requested_sink_name,
                                    const string &requested_sink_type) {
	string shell_sink_name;
	{
		annotated_unique_lock<annotated_mutex> lck(lock);
		auto sink_name = StringUtil::Lower(requested_sink_name);
		auto sink_type = StringUtil::Lower(requested_sink_type);
		auto entry = GetLogSinkEntryInternal(sink_name);
		if (sink_type.empty()) {
			sink_type = entry ? entry->type : sink_name;
		}
		// an enabled stdout sink already prints the log entries
		if (sink_type != LogConfig::STDOUT_STORAGE_NAME ||
		    (entry && entry->type == sink_type && entry->filter.enabled)) {
			return;
		}
		auto prints = [&](const string &other_name, const LogSinkEntry &other) {
			return other_name != sink_name && other.type == SHELL_LOG_SINK_NAME && other.filter.enabled;
		};
		if (prints(LogConfig::DEFAULT_LOG_SINK_NAME, default_log_sink)) {
			shell_sink_name = LogConfig::DEFAULT_LOG_SINK_NAME;
		}
		for (auto &other : named_log_sinks) {
			if (prints(other.first, other.second)) {
				shell_sink_name = other.first;
			}
		}
	}
	if (shell_sink_name == LogConfig::DEFAULT_LOG_SINK_NAME) {
		DUCKDB_LOG_WARNING(db,
		                   "The 'stdout' log sink and the default log sink, which is '%s', both print the log "
		                   "entries to the console.\nTo print them once, run CALL disable_logging().",
		                   SHELL_LOG_SINK_NAME);
	} else if (!shell_sink_name.empty()) {
		DUCKDB_LOG_WARNING(db,
		                   "The 'stdout' log sink and the '%s' log sink both print the log entries to the console.\nTo "
		                   "print them once, run CALL disable_log_sink('%s').",
		                   shell_sink_name, shell_sink_name);
	}
}

void LogManager::SetDefaultLogSink(DatabaseInstance &db, const string &requested_sink_type) {
	auto sink_type = StringUtil::Lower(ResolveDeprecatedLogSinkName(db, requested_sink_type));
	WarnIfPrintedTwice(db, LogConfig::DEFAULT_LOG_SINK_NAME, sink_type);

	annotated_unique_lock<annotated_mutex> lck(lock);
	// a setting cannot supply the path that the file sink requires, so reject the switch here (active sink preserved)
	// instead of installing a path-less sink that throws on every later flush
	if (sink_type == LogConfig::FILE_STORAGE_NAME && default_log_sink.type != sink_type) {
		throw InvalidConfigurationException(
		    "The 'file' log sink requires a path, use CALL enable_logging(type := 'file', config := {path: '...'}) "
		    "instead");
	}
	SetDefaultLogSinkInternal(db, sink_type, default_log_sink.filter);
	UpdateLoggerConfigInternal();
}

void LogManager::SetDefaultLogSinkInternal(DatabaseInstance &db, const string &sink_type, LogConfig filter) {
	auto sink_type_to_lower = StringUtil::Lower(sink_type);

	// keep the lowercased type, so that it is recognized when set again
	filter.storage = sink_type_to_lower;
	if (default_log_sink.type == sink_type_to_lower) {
		default_log_sink.filter = std::move(filter);
		return;
	}

	auto new_sink = CreateLogSinkInternal(db, sink_type_to_lower);
	// a registered sink exists once: when a named sink holds it, it moves to the default log sink
	auto entry_name = GetLogSinkEntryNameInternal(*new_sink);
	if (!entry_name.empty()) {
		named_log_sinks.erase(entry_name);
	}

	// Flush the old sink, we are going to replace it.
	default_log_sink.sink->FlushAll();
	default_log_sink =
	    LogSinkEntry {sink_type_to_lower, std::move(new_sink), case_insensitive_map_t<Value>(), std::move(filter)};
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
		if (fs.SubSystemIsDisabled("LocalFileSystem")) {
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
static void RememberSinkConfig(LogSinkEntry &sink, const case_insensitive_map_t<Value> &sink_config) {
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

void LogManager::VerifyLogFileUnusedInternal(const string &sink_name, const string &path) {
	if (path.empty()) {
		return;
	}
	// a disabled sink does not hold on to its file
	auto verify_sink = [&](const string &other_name, const LogSinkEntry &other) {
		if (StringUtil::CIEquals(other_name, sink_name) || other.type != LogConfig::FILE_STORAGE_NAME ||
		    !other.filter.enabled) {
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

//! The options of the built-in sinks of this type. They also take only_flush_on_full_buffer, which is for testing
static vector<string> GetBuiltInLogSinkOptions(const string &sink_type) {
	if (sink_type == LogConfig::IN_MEMORY_STORAGE_NAME) {
		return {"buffer_size"};
	}
	if (sink_type == LogConfig::STDOUT_STORAGE_NAME) {
		return {"buffer_size", "delim"};
	}
	return {"path", "normalize", "buffer_size", "delim"};
}

void LogManager::VerifyLogSinkConfigInternal(const string &sink_name, const string &sink_type,
                                             const case_insensitive_map_t<Value> &sink_config) {
	if (!LogConfig::IsBuiltInLogSink(sink_type)) {
		return;
	}
	auto options = GetBuiltInLogSinkOptions(sink_type);
	for (auto &entry : sink_config) {
		auto option = StringUtil::Lower(entry.first);
		if (option == "only_flush_on_full_buffer" ||
		    std::find(options.begin(), options.end(), option) != options.end()) {
			continue;
		}
		auto known_options = options.size() == 1 ? "Its only option is " + options[0]
		                                         : "Its options are " + StringUtil::Join(options, ", ");
		throw InvalidInputException("Log sink '%s' is a '%s' log sink, which has no option '%s'. %s", sink_name,
		                            sink_type, entry.first, known_options);
	}
}

void LogManager::UpdateLogSinkConfigInternal(DatabaseInstance &db, const string &sink_name, LogSinkEntry &entry,
                                             const case_insensitive_map_t<Value> &sink_config) {
	if (sink_config.empty()) {
		return;
	}
	VerifyLogSinkConfigInternal(sink_name, entry.type, sink_config);
	auto config_copy = sink_config;
	entry.sink->UpdateConfig(db, config_copy);
	RememberSinkConfig(entry, sink_config);
}

void LogManager::UpdateLogSinkFilterInternal(LogConfig &filter, const LogSinkFilterUpdate &filter_update) {
	if (filter_update.set_log_types) {
		filter.enabled_log_types.clear();
		if (filter_update.log_types.empty()) {
			filter.mode = LogMode::LEVEL_ONLY;
		} else {
			auto min_log_level = LogLevel::LOG_FATAL;
			for (auto &log_type : filter_update.log_types) {
				auto lookup = LookupLogTypeInternal(log_type);
				if (!lookup) {
					throw InvalidInputException("Unknown log type: '%s'", log_type);
				}
				filter.enabled_log_types.insert(lookup->name);
				min_log_level = MinValue(min_log_level, lookup->level);
			}
			filter.mode = LogMode::ENABLE_SELECTED;
			// the log types are logged at their own level, unless a level is given
			filter.level = min_log_level;
		}
	}
	if (filter_update.set_level) {
		filter.level = filter_update.level;
	}
}

//! Throws unless a sink of this type can be named like this. The name of a type of sink is reserved for that type
static void VerifyLogSinkName(const string &sink_name, const string &sink_type,
                              const case_insensitive_map_t<shared_ptr<LogSink>> &registered_log_sinks) {
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
                               const string &requested_sink_type, const case_insensitive_map_t<Value> &sink_config,
                               const LogSinkFilterUpdate &filter_update) {
	auto sink_name = StringUtil::Lower(ResolveDeprecatedLogSinkName(db, requested_sink_name));
	auto sink_type = requested_sink_type.empty()
	                     ? string()
	                     : StringUtil::Lower(ResolveDeprecatedLogSinkName(db, requested_sink_type));
	const bool is_default = sink_name == LogConfig::DEFAULT_LOG_SINK_NAME;

	annotated_unique_lock<annotated_mutex> lck(lock);
	auto entry = GetLogSinkEntryInternal(sink_name);
	if (sink_type.empty()) {
		// an existing sink keeps its type, a new sink without a type is named after its type
		sink_type = entry ? entry->type : sink_name;
	}
	if (!is_default) {
		VerifyLogSinkName(sink_name, sink_type, registered_log_sinks);
	}
	// a sink that gets another type is a new sink, which replaces an enabled named sink only once it is disabled
	const bool is_new = !entry || entry->type != sink_type;
	if (entry && is_new && !is_default && entry->filter.enabled) {
		throw InvalidInputException("Log sink '%s' is a '%s' log sink, disable it before enabling a '%s' log sink "
		                            "with this name",
		                            sink_name, entry->type, sink_type);
	}
	if (sink_type == LogConfig::FILE_STORAGE_NAME) {
		auto path = GetSinkConfigPath(sink_config);
		if (is_new && path.empty()) {
			if (is_default) {
				throw InvalidInputException("Cannot enable the 'file' log sink without a valid path. Provide one in "
				                            "config, e.g. CALL enable_logging(type := 'file', config := {path: "
				                            "'mylog.csv'});");
			}
			auto type_parameter = sink_name == sink_type ? string() : StringUtil::Format(", type='%s'", sink_type);
			throw InvalidInputException(
			    "The 'file' log sink requires a path, e.g. CALL enable_log_sink('%s'%s, config={path: 'log.csv'})",
			    sink_name, type_parameter);
		}
		VerifyLogFileUnusedInternal(sink_name, path.empty() ? GetSinkConfigPath(entry->config) : path);
	}
	VerifyLogSinkConfigInternal(sink_name, sink_type, sink_config);

	// a failed change puts the sinks back as they were
	auto previous_default_log_sink = default_log_sink;
	auto previous_named_log_sinks = named_log_sinks;
	try {
		if (!is_new) {
			auto filter = entry->filter;
			if (filter_update.reset) {
				filter = LogConfig();
				filter.storage = entry->type;
			}
			UpdateLogSinkFilterInternal(filter, filter_update);
			UpdateLogSinkConfigInternal(db, sink_name, *entry, sink_config);
			filter.enabled = true;
			entry->filter = std::move(filter);
		} else {
			// a new sink logs every log type at the default level, unless told otherwise
			LogConfig filter;
			filter.enabled = true;
			UpdateLogSinkFilterInternal(filter, filter_update);
			if (is_default) {
				SetDefaultLogSinkInternal(db, sink_type, std::move(filter));
				UpdateLogSinkConfigInternal(db, sink_name, default_log_sink, sink_config);
			} else {
				auto sink = CreateLogSinkInternal(db, sink_type);
				// a registered sink exists once, so it has one name only
				auto entry_name = GetLogSinkEntryNameInternal(*sink);
				if (!entry_name.empty() && entry_name != sink_name) {
					throw InvalidInputException("Log sink '%s' is already in use as '%s', a registered log sink can "
					                            "only have one name",
					                            sink_type, entry_name);
				}
				filter.storage = sink_type;
				LogSinkEntry new_entry {sink_type, std::move(sink), case_insensitive_map_t<Value>(), std::move(filter)};
				UpdateLogSinkConfigInternal(db, sink_name, new_entry, sink_config);
				if (entry) {
					entry->sink->FlushAll();
				}
				named_log_sinks[sink_name] = std::move(new_entry);
			}
		}
	} catch (...) {
		default_log_sink = std::move(previous_default_log_sink);
		named_log_sinks = std::move(previous_named_log_sinks);
		throw;
	}
	UpdateLoggerConfigInternal();
}

void LogManager::DisableLogSink(DatabaseInstance &db, const string &requested_sink_name) {
	auto sink_name = StringUtil::Lower(ResolveDeprecatedLogSinkName(db, requested_sink_name));

	annotated_unique_lock<annotated_mutex> lck(lock);
	auto entry = GetLogSinkEntryInternal(sink_name);
	if (!entry) {
		throw LogSinkDoesNotExistInternal(requested_sink_name);
	}
	if (!entry->filter.enabled) {
		return;
	}
	entry->sink->FlushAll();
	entry->filter.enabled = false;
	UpdateLoggerConfigInternal();
}

void LogManager::UpdateDefaultLogSinkConfig(DatabaseInstance &db, case_insensitive_map_t<Value> &config_value) {
	annotated_unique_lock<annotated_mutex> lck(lock);
	if (default_log_sink.type == LogConfig::FILE_STORAGE_NAME) {
		VerifyLogFileUnusedInternal(LogConfig::DEFAULT_LOG_SINK_NAME, GetSinkConfigPath(config_value));
	}
	UpdateLogSinkConfigInternal(db, LogConfig::DEFAULT_LOG_SINK_NAME, default_log_sink, config_value);
}

void LogManager::SetEnableStructuredLoggers(vector<string> &enabled_logger_types) {
	annotated_unique_lock<annotated_mutex> lck(lock);
	auto filter = default_log_sink.filter;
	LogSinkFilterUpdate filter_update;
	filter_update.set_log_types = true;
	filter_update.log_types = enabled_logger_types;
	UpdateLogSinkFilterInternal(filter, filter_update);
	filter.enabled = true;
	default_log_sink.filter = std::move(filter);
	UpdateLoggerConfigInternal();
}

void LogManager::TruncateLogSink(const string &name) {
	annotated_unique_lock<annotated_mutex> lck(lock);
	GetLogSinkInternal(name)->Truncate();
}

bool LogManager::RegisterLogStorage(const string &name, shared_ptr<LogSink> &storage) {
	return RegisterLogSink(name, storage);
}

shared_ptr<LogSink> LogManager::GetLogStorage() {
	return GetDefaultLogSink();
}

void LogManager::SetLogStorage(DatabaseInstance &db, const string &storage_name) {
	SetDefaultLogSink(db, storage_name);
}

void LogManager::UpdateLogStorageConfig(DatabaseInstance &db, case_insensitive_map_t<Value> &config_value) {
	UpdateDefaultLogSinkConfig(db, config_value);
}

void LogManager::TruncateLogStorage() {
	TruncateLogSink(string());
}

LogConfig LogManager::GetConfig() {
	annotated_unique_lock<annotated_mutex> lck(lock);
	return default_log_sink.filter;
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

void LogManager::UpdateLoggerConfigInternal() {
	config = make_shared_ptr<const LogConfig>(CombineLogSinkFilters(default_log_sink, named_log_sinks));
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
