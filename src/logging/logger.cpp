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

LogStorage::LogStorage(shared_ptr<DatabaseInstance> &db_p) : buffer(make_uniq<DataChunk>()) {
	// TODO: add context to schema
	// LogEntry Schema
	vector<LogicalType> types = {
		LogicalType::VARCHAR, // level
		LogicalType::VARCHAR, // log_type
		LogicalType::VARCHAR, // message
	};
	max_buffer_size = 1;
	buffer->Initialize(Allocator::DefaultAllocator(), types, max_buffer_size);
	log_entries = make_uniq<ColumnDataCollection>(Allocator::DefaultAllocator(), types);
}

LogStorage::~LogStorage() = default;

void LogStorage::WriteLogEntry(LogLevel level, const string& log_type, const string& log_message, const LoggingContext& context) {
	auto size = buffer->size();
	auto level_data = FlatVector::GetData<string_t>(buffer->data[0]);
	auto type_data = FlatVector::GetData<string_t>(buffer->data[1]);
	auto message_data = FlatVector::GetData<string_t>(buffer->data[2]);

	level_data[size] = StringVector::AddString(buffer->data[0], EnumUtil::ToString(level));
	type_data[size] = StringVector::AddString(buffer->data[1], log_type);
	message_data[size] = StringVector::AddString(buffer->data[2], log_message);

	// TODO: write context
	buffer->SetCardinality(size+1);

	if (size+1 >= max_buffer_size) {
		Flush();
	}
}

void LogStorage::WriteLogEntries(DataChunk &chunk, const LoggingContext &context) {
	// TODO: figure out what schema the input chunks should have here
	log_entries->Append(chunk);
}

void LogStorage::Flush() {
	log_entries->Append(*buffer);
	buffer->Reset();
}

LogConfig::LogConfig() : mode(LogMode::DISABLED), level(LogLevel::WARNING) {
}

bool LogConfig::IsConsistent() const {
	if (mode == LogMode::DISABLED || mode == LogMode::LEVEL_ONLY) {
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

LogConfig LogConfig::Create(LogLevel level) {
	return LogConfig(level, LogMode::LEVEL_ONLY, nullptr, nullptr);
}
LogConfig LogConfig::CreateFromEnabled(LogLevel level, unordered_set<string> &enabled_loggers) {
	return LogConfig(level, LogMode::ENABLE_SELECTED, enabled_loggers, nullptr);
}

LogConfig LogConfig::CreateFromDisabled(LogLevel level, unordered_set<string> &disabled_loggers) {
	return LogConfig(level, LogMode::DISABLE_SELECTED, nullptr, disabled_loggers);
}

LogConfig::LogConfig(LogLevel level_p, LogMode mode_p, optional_ptr<unordered_set<string>> enabled_loggers_p, optional_ptr<unordered_set<string>> disabled_loggers_p)
:  mode(mode_p), level(level_p), enabled_loggers(enabled_loggers_p), disabled_loggers(disabled_loggers_p) {
}

Logger &Logger::Get(DatabaseInstance &db) {
	return db.GetLogManager().GlobalLogger();
}

Logger& Logger::Get(ThreadContext &thread_context) {
	return *thread_context.logger;
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

void ThreadSafeLogger::Log(LogLevel log_level, const string &log_type, const string &log_message) {
	if (config.Level() < log_level) {
		return;
	}
	if (config.Mode() == LogMode::ENABLE_SELECTED && config.EnabledLoggers().find(log_type) == config.EnabledLoggers().end()) {
		return;
	}
	if (config.Mode() == LogMode::DISABLE_SELECTED && config.DisabledLoggers().find(log_type) != config.DisabledLoggers().end()) {
		return;
	}

	manager->WriteLogEntry(log_level, log_type, log_message, context);
}

void ThreadSafeLogger::Log(LogLevel log_level, const string &log_message) {
	if (config.Level() < log_level) {
		return;
	}
	if (config.Mode() != LogMode::LEVEL_ONLY) {
		return;
	}
	manager->WriteLogEntry(log_level, context.default_log_type, log_message, context);
}

void ThreadSafeLogger::Flush() {
	// NOP
}

void ThreadLocalLogger::Log(LogLevel log_level, const string &log_type, const string &log_message) {
	throw NotImplementedException("ThreadLocalLogger::Log");
}

void ThreadLocalLogger::Log(LogLevel log_level, const string &log_message) {
	throw NotImplementedException("ThreadLocalLogger::Log");
}

void ThreadLocalLogger::Flush() {
	throw NotImplementedException("");
}

void MutableLogger::UpdateConfig(LogConfig &new_config) {
	unique_lock<mutex> lck(lock);
	config = new_config;

	// Update atomics for lock-free access
	level = config.Level();
	mode = config.Mode();
}

void MutableLogger::Log(LogLevel log_level, const string &log_type, const string &log_message) {
	// check atomic mode to early out if disabled
	if (mode == LogMode::DISABLED) {
		return;
	}

	// check atomic level to early out if level too low
	if (level < log_level) {
		return;
	}

	if (config.Mode() == LogMode::LEVEL_ONLY) {
		manager->WriteLogEntry(log_level, log_type, log_message, context);
	}

	// ENABLE_SELECTED and DISABLE_SELECTED are expensive and need full global lock TODO: can we do better here?
	bool should_log = false;
	{
		unique_lock<mutex> lck(lock);
		if (config.Mode() == LogMode::ENABLE_SELECTED && config.EnabledLoggers().find(log_type) == config.EnabledLoggers().end()) {
			should_log = true;
		} else if (config.Mode() == LogMode::DISABLE_SELECTED && config.DisabledLoggers().find(log_type) != config.DisabledLoggers().end()) {
			should_log = true;
		}
	}
	if (should_log) {
		manager->WriteLogEntry(log_level, log_type, log_message, context);
	}
}

void MutableLogger::Log(LogLevel log_level, const string &log_message) {
	// check atomic mode to early out if disabled
	if (mode != LogMode::LEVEL_ONLY) {
		return;
	}

	// check atomic level to early out if level too low
	if (level < log_level) {
		return;
	}

	manager->WriteLogEntry(log_level, context.default_log_type, log_message, context);
}

void MutableLogger::Flush() {
	// NOP
}

unique_ptr<Logger> LogManager::CreateLogger(LoggingContext &context, bool thread_safe, bool mutable_settings) {
	unique_lock<mutex> lck(lock);
	if (!config.IsConsistent()) {
		throw InvalidConfigurationException("Log configuration is inconsistent");
	}
	if (mutable_settings) {
		return make_uniq<MutableLogger>(config, context, *this);
	}
	if (config.Mode() == LogMode::DISABLED) {
		return make_uniq<NopLogger>();
	}
	if (!thread_safe) {
		return make_uniq<ThreadLocalLogger>(config, context, *this);
	}
	return make_uniq<ThreadSafeLogger>(config, context, *this);
}

Logger &LogManager::GlobalLogger() {
	return *global_logger;
}

LogManager::LogManager(shared_ptr<DatabaseInstance> &db, LogConfig config_p) : config(config_p) {
	log_storage = make_uniq<LogStorage>(db);
}

void LogManager::Initialize() {
	LoggingContext context;
	context.default_log_type = "global_logger";
	global_logger = CreateLogger(context, true, true);
}

LogManager &LogManager::Get(ClientContext &context) {
	return context.db->GetLogManager();
}

void LogManager::WriteLogEntry(LogLevel log_level, const string &log_type, const string &log_message, const LoggingContext &context) {
	unique_lock<mutex> lck(lock);
	log_storage->WriteLogEntry(log_level, log_type, log_message, context);
}

void LogManager::FlushCachedLogEntries(DataChunk &chunk, const LoggingContext &context) {
	throw NotImplementedException("FlushCachedLogEntries");
}

} // namespace duckdb
