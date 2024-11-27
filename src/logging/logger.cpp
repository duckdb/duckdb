#include "duckdb/logging/logger.hpp"
#include "duckdb/main/database.hpp"
#include "duckdb/main/connection.hpp"
#include "duckdb/main/table_description.hpp"
#include "duckdb/common/types/data_chunk.hpp"
#include "duckdb/common/types/column/column_data_collection.hpp"

namespace duckdb {

LogStorage::LogStorage(shared_ptr<DatabaseInstance> &db_p) : buffer(make_uniq<DataChunk>()) {
	// TODO: add context to schema
	vector<LogicalType> types = {
		LogicalType::VARCHAR, // level
		LogicalType::VARCHAR, // log_type
		LogicalType::VARCHAR, // message
	};
	max_buffer_size = STANDARD_VECTOR_SIZE;
	buffer->InitializeEmpty(types);
	buffer->SetCapacity(max_buffer_size);
	buffer_size = 0;
	log_entries = make_uniq<ColumnDataCollection>(db_p->GetBufferManager(), types);
}

LogStorage::~LogStorage() = default;

void LogStorage::WriteLogEntry(LogLevel level, const string& log_type, const string& log_message, const LoggingContext& context) {
	auto level_data = FlatVector::GetData<string_t>(buffer->data[0]);
	auto type_data = FlatVector::GetData<string_t>(buffer->data[1]);
	auto message_data = FlatVector::GetData<string_t>(buffer->data[2]);

	level_data[buffer_size] = StringVector::AddString(buffer->data[0], EnumUtil::ToString(level));
	type_data[buffer_size] = StringVector::AddString(buffer->data[1], log_type);
	message_data[buffer_size] = StringVector::AddString(buffer->data[2], log_message);

	// TODO: write context

	buffer_size++;

	if (buffer_size >= max_buffer_size) {
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

void ThreadSafeLogger::Flush() {
	// NOP
}

void ThreadLocalLogger::Log(LogLevel log_level, const string &log_type, const string &log_message) {
	throw NotImplementedException("");
}

void ThreadLocalLogger::Flush() {
	throw NotImplementedException("");
}

unique_ptr<Logger> LogManager::CreateLogger(LoggingContext &context, bool thread_safe) {
	unique_lock<mutex> lck(lock);
	if (!config.IsConsistent()) {
		throw InvalidConfigurationException("Log configuration is inconsistent");
	}
	if (config.Mode() == LogMode::DISABLED) {
		return make_uniq<NopLogger>();
	}
	if (!thread_safe) {
		return make_uniq<ThreadLocalLogger>(config, context);
	}
	return make_uniq<ThreadLocalLogger>(config, context);
}

LogManager::LogManager(shared_ptr<DatabaseInstance> &db, LogConfig config_p) : config(config_p) {
	log_storage = make_uniq<LogStorage>(db);
}

void LogManager::WriteLogEntry(LogLevel log_level, const string &log_type, const string &log_message, const LoggingContext &context) {
	unique_lock<mutex> lck(lock);
	log_storage->WriteLogEntry(log_level, log_type, log_message, context);
}

void LogManager::FlushCachedLogEntries(DataChunk &chunk, const LoggingContext &context) {
	throw NotImplementedException("FlushCachedLogEntries");
}

} // namespace duckdb
