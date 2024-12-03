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

LogStorage::LogStorage(shared_ptr<DatabaseInstance> &db_p) : entry_buffer(make_uniq<DataChunk>()), log_context_buffer(make_uniq<DataChunk>()) {
	// LogEntry Schema
	vector<LogicalType> log_entry_schema = {
		LogicalType::UBIGINT,   // context_id
		LogicalType::TIMESTAMP, // timestamp
		LogicalType::VARCHAR,   // log_type TODO: const vector where possible?
		LogicalType::VARCHAR,   // level TODO: enumify
		LogicalType::VARCHAR,   // message
	};

	// LogContext Schema
	vector<LogicalType> log_context_schema = {
		LogicalType::UBIGINT, // context_id
		LogicalType::VARCHAR, // scope TODO: enumify
		LogicalType::UBIGINT, // client_context
		LogicalType::UBIGINT, // transaction_id
		LogicalType::UBIGINT, // thread
	};

	max_buffer_size = 1;
	entry_buffer->Initialize(Allocator::DefaultAllocator(), log_entry_schema, max_buffer_size);
	log_context_buffer->Initialize(Allocator::DefaultAllocator(), log_context_schema, max_buffer_size);
	log_entries = make_uniq<ColumnDataCollection>(Allocator::DefaultAllocator(), log_entry_schema);
	log_contexts = make_uniq<ColumnDataCollection>(Allocator::DefaultAllocator(), log_context_schema);
}

LogStorage::~LogStorage() = default;

void LogStorage::WriteLogEntry(timestamp_t timestamp, LogLevel level, const string& log_type, const string& log_message, const RegisteredLoggingContext& context) {
	auto size = entry_buffer->size();
	auto context_id_data = FlatVector::GetData<idx_t>(entry_buffer->data[0]);
	auto timestamp_data = FlatVector::GetData<timestamp_t>(entry_buffer->data[1]);
	auto type_data = FlatVector::GetData<string_t>(entry_buffer->data[2]);
	auto level_data = FlatVector::GetData<string_t>(entry_buffer->data[3]);
	auto message_data = FlatVector::GetData<string_t>(entry_buffer->data[4]);

	context_id_data[size] = context.context_id;
	timestamp_data[size] = timestamp;
	type_data[size] = StringVector::AddString(entry_buffer->data[2], log_type);
	level_data[size] = StringVector::AddString(entry_buffer->data[3], EnumUtil::ToString(level));
	message_data[size] = StringVector::AddString(entry_buffer->data[4], log_message);

	entry_buffer->SetCardinality(size+1);

	if (size+1 >= max_buffer_size) {
		Flush();
	}
}

void LogStorage::WriteLogEntries(DataChunk &chunk, const RegisteredLoggingContext &context) {
	log_entries->Append(chunk);
}

void LogStorage::Flush() {
	log_entries->Append(*entry_buffer);
	entry_buffer->Reset();

	log_contexts->Append(*log_context_buffer);
	log_context_buffer->Reset();
}

void LogStorage::WriteLoggingContext(RegisteredLoggingContext &context) {
	auto size = log_context_buffer->size();

	auto context_id_data = FlatVector::GetData<idx_t>(log_context_buffer->data[0]);
	context_id_data[size] = context.context_id;

	auto context_scope_data = FlatVector::GetData<string_t>(log_context_buffer->data[1]);
	context_scope_data[size] = StringVector::AddString(log_context_buffer->data[1], EnumUtil::ToString(context.context.scope));

	if (context.context.client_context.IsValid()) {
		auto client_context_data = FlatVector::GetData<idx_t>(log_context_buffer->data[2]);
		client_context_data[size] = context.context.client_context.GetIndex();
	} else {
		FlatVector::Validity(log_context_buffer->data[2]).SetInvalid(size);
	}
	if (context.context.transaction_id.IsValid()) {
		auto client_context_data = FlatVector::GetData<idx_t>(log_context_buffer->data[3]);
		client_context_data[size] = context.context.transaction_id.GetIndex();
	} else {
		FlatVector::Validity(log_context_buffer->data[3]).SetInvalid(size);
	}
	if (context.context.thread.IsValid()) {
		auto thread_data = FlatVector::GetData<idx_t>(log_context_buffer->data[4]);
		thread_data[size] = context.context.thread.GetIndex();
	} else {
		FlatVector::Validity(log_context_buffer->data[4]).SetInvalid(size);
	}

	log_context_buffer->SetCardinality(size + 1);

	if (size + 1 >= max_buffer_size) {
		Flush();
	}
}
LogConfig::LogConfig() : enabled(false), mode(LogMode::LEVEL_ONLY), level(LogLevel::INFO) {
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

LogConfig::LogConfig(bool enabled, LogLevel level_p, LogMode mode_p, optional_ptr<unordered_set<string>> enabled_loggers_p, optional_ptr<unordered_set<string>> disabled_loggers_p)
:  enabled(enabled), mode(mode_p), level(level_p), enabled_loggers(enabled_loggers_p), disabled_loggers(disabled_loggers_p) {
	output = LogDestinationType::IN_MEMORY;
}

Logger &Logger::Get(DatabaseInstance &db) {
	return db.GetLogManager().GlobalLogger();
}

Logger& Logger::Get(ThreadContext &thread_context) {
	return *thread_context.logger;
}

Logger& Logger::Get(ExecutionContext &execution_context) {
	return *execution_context.thread.logger;
}

Logger& Logger::Get(ClientContext &client_context) {
	return client_context.GetLogger();
}

Logger& Logger::Get(FileOpener &opener) {
	auto maybe_context = opener.TryGetClientContext();
	if (maybe_context) {
		Get(*maybe_context);
	}
	auto maybe_db = opener.TryGetDatabase();
	if (maybe_db) {
		Get(*maybe_db);
	}
	throw NotImplementedException("Logger::Get(FileOpener)");
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

void Logger::Log(const char *log_type, LogLevel log_level, std::function<string()> callback) {
	if (ShouldLog(log_type, log_level)) {
		auto string = callback();
		WriteLog(log_type, log_level, string.c_str());
	}
}
void Logger::Log(LogLevel log_level, std::function<string()> callback) {
	if (ShouldLog(log_level)) {
		auto string = callback();
		WriteLog(log_level, string.c_str());
	}
}

ThreadSafeLogger::ThreadSafeLogger(LogConfig &config_p, LoggingContext &context_p, LogManager &manager)
		: Logger(manager), config(config_p), context(manager.RegisterLoggingContext(context_p)) {
	// NopLogger should be used instead
	D_ASSERT(config_p.enabled);
}

bool ThreadSafeLogger::ShouldLog(const char *log_type, LogLevel log_level) {
	if (config.level > log_level) {
		return false;
	}
	if (config.mode == LogMode::ENABLE_SELECTED) {
		return config.enabled_loggers.find(log_type) != config.enabled_loggers.end();
	}
	if (config.mode == LogMode::DISABLE_SELECTED) {
		return config.disabled_loggers.find(log_type) == config.disabled_loggers.end();
	}
	return true;
}

// TODO: is this desirable?
bool ThreadSafeLogger::ShouldLog(LogLevel log_level) {
	return ShouldLog(context.context.default_log_type, log_level);
}

void ThreadSafeLogger::WriteLog(const char *log_type, LogLevel log_level, const char *log_message) {
	manager->WriteLogEntry(Timestamp::GetCurrentTimestamp(), log_type, log_level, log_message, context);
}

void ThreadSafeLogger::WriteLog(LogLevel log_level, const char *log_message) {
	manager->WriteLogEntry(Timestamp::GetCurrentTimestamp(), context.context.default_log_type, log_level, log_message, context);
}

void ThreadSafeLogger::Flush() {
	// NOP
}

ThreadLocalLogger::ThreadLocalLogger(LogConfig &config_p, LoggingContext &context_p, LogManager &manager)
		: Logger(manager), config(config_p), context(manager.RegisterLoggingContext(context_p)) {
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
	throw NotImplementedException("");
}

MutableLogger::MutableLogger(LogConfig &config_p, LoggingContext &context_p, LogManager &manager)
		: Logger(manager), config(config_p), context(manager.RegisterLoggingContext(context_p)) {
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
	manager->WriteLogEntry(Timestamp::GetCurrentTimestamp(), log_type, log_level, log_message, context);
}

void MutableLogger::WriteLog(LogLevel log_level, const char *log_message) {
	manager->WriteLogEntry(Timestamp::GetCurrentTimestamp(), context.context.default_log_type, log_level, log_message, context);
}

bool MutableLogger::ShouldLog(const char *log_type, LogLevel log_level) {
	if (!enabled) {
		return false;
	}

	// check atomic level to early out if level too low
	if (level > log_level) {
		return false;
	}

	if (config.mode == LogMode::LEVEL_ONLY) {
		return true;
	}

	// ENABLE_SELECTED and DISABLE_SELECTED are expensive and need full global lock TODO: can we do better here?
	{
		unique_lock<mutex> lck(lock);
		if (config.mode == LogMode::ENABLE_SELECTED) {
			return config.enabled_loggers.find(log_type) != config.enabled_loggers.end();
		} else if (config.mode == LogMode::DISABLE_SELECTED) {
			return config.disabled_loggers.find(log_type) == config.disabled_loggers.end();
		}
	}
	throw InternalException("Should be unreachable (MutableLogger::ShouldLog)");
}

bool MutableLogger::ShouldLog(LogLevel log_level) {
	if (!enabled) {
		return false;
	}

	// check atomic level to early out if level too low
	if (level > log_level) {
		return false;
	}

	// TODO: this checks things twice
	return ShouldLog(context.context.default_log_type, log_level);
}

void MutableLogger::Flush() {
	// NOP
}

unique_ptr<Logger> LogManager::CreateLogger(LoggingContext &context, bool thread_safe, bool mutable_settings) {
	// Make a copy of the config holding the lock
	LogConfig config_copy;
	{
		unique_lock<mutex> lck(lock);
		config_copy = config;
	}

	if (mutable_settings) {
		return make_uniq<MutableLogger>(config_copy, context, *this);
	}
	if (!config_copy.enabled) {
		return make_uniq<NopLogger>();
	}
	if (!thread_safe) {
		// TODO: implement ThreadLocalLogger and return it here
		// return make_uniq<ThreadLocalLogger>(config_copy, context, *this);
	}
	return make_uniq<ThreadSafeLogger>(config_copy, context, *this);
}

RegisteredLoggingContext LogManager::RegisterLoggingContext(LoggingContext &context) {
	unique_lock<mutex> lck(lock);

	// TODO: can this realistically happen?
	if (registered_log_contexts.find(next_registered_logging_context_index) != registered_log_contexts.end()) {
		throw InternalException("LogManager ran out of available LoggingContext indices!");
	}

	auto res = registered_log_contexts.insert({next_registered_logging_context_index, context});

	next_registered_logging_context_index++;

	RegisteredLoggingContext result = {
		res.first->first,
		res.first->second
	};

	log_storage->WriteLoggingContext(result);

	return result;
}

void LogManager::DropLoggingContext(RegisteredLoggingContext &context) {
	if (registered_log_contexts.find(context.context_id) != registered_log_contexts.end()) {
		registered_log_contexts.erase(context.context_id);
	}
}

Logger &LogManager::GlobalLogger() {
	return *global_logger;
}

LogManager::LogManager(shared_ptr<DatabaseInstance> &db, LogConfig config_p) : config(config_p) {
	log_storage = make_uniq<LogStorage>(db);
}

void LogManager::Initialize() {
	LoggingContext context(LogContextScope::DATABASE);
	global_logger = CreateLogger(context, true, true);
}

LogManager &LogManager::Get(ClientContext &context) {
	return context.db->GetLogManager();
}

void LogManager::WriteLogEntry(timestamp_t timestamp, const char *log_type, LogLevel log_level, const char *log_message, const RegisteredLoggingContext &context) {
	unique_lock<mutex> lck(lock);
	log_storage->WriteLogEntry(timestamp, log_level, log_type, log_message, context);
}

void LogManager::FlushCachedLogEntries(DataChunk &chunk, const RegisteredLoggingContext &context) {
	throw NotImplementedException("FlushCachedLogEntries");
}

void LogManager::SetEnableLogging(bool enable) {
	unique_lock<mutex> lck(lock);
	config.enabled = enable;
	global_logger->UpdateConfig(config);
}

void LogManager::SetLogMode(LogMode mode) {
	unique_lock<mutex> lck(lock);
	config.mode = mode;
	global_logger->UpdateConfig(config);
}

void LogManager::SetLogLevel(LogLevel level) {
	unique_lock<mutex> lck(lock);
	config.level = level;
	global_logger->UpdateConfig(config);
}

void LogManager::SetEnabledLoggers(unordered_set <string> &enabled_loggers) {
	unique_lock<mutex> lck(lock);
	config.enabled_loggers = enabled_loggers;
	global_logger->UpdateConfig(config);
}

void LogManager::SetDisabledLoggers(unordered_set <string> &disabled_loggers) {
	unique_lock<mutex> lck(lock);
	config.disabled_loggers = disabled_loggers;
	global_logger->UpdateConfig(config);
}

LogConfig LogManager::GetConfig() {
	unique_lock<mutex> lck(lock);
	return config;
}


} // namespace duckdb
