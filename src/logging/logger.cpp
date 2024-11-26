#include "duckdb/logging/logger.hpp"
#include "duckdb/main/database.hpp"
#include "duckdb/common/enable_shared_from_this_ipp.hpp"
#include "duckdb/main/connection.hpp"
#include "duckdb/main/table_description.hpp"

namespace duckdb {

LogWriter::LogWriter(shared_ptr<DatabaseInstance> &db_p) {
	vector<LogicalType> types = {
		LogicalType::VARCHAR, // level
		LogicalType::VARCHAR, // message
	};
	db = db_p;
	buffer = DataChunk();
	buffer.InitializeEmpty(types);
	buffer.SetCapacity(max_buffer_size);
	buffer_size = 0;
	max_buffer_size = 10 * STANDARD_VECTOR_SIZE;
	description = make_uniq<TableDescription>(INVALID_CATALOG, DEFAULT_SCHEMA, "duckdb_log_output");
}

void LogWriter::Log(LogLevel level, const string& log_message, optional_ptr<LoggingContext> context) {
	auto level_data = FlatVector::GetData<string_t>(buffer.data[0]);
	auto message_data = FlatVector::GetData<string_t>(buffer.data[1]);

	level_data[buffer_size] = StringVector::AddString(buffer.data[0], EnumUtil::ToString(level));
	message_data[buffer_size] = StringVector::AddString(buffer.data[1], log_message);

	buffer_size++;

	if (buffer_size >= max_buffer_size) {
		Flush();
	}
}

void LogWriter::Flush() {
	auto db_locked = db.lock();
	if (db_locked) {
		auto conn = Connection(*db_locked);
		conn.Append(*description, buffer);
	}
	buffer.Reset();
}

LoggingManager::LoggingManager(shared_ptr<DatabaseInstance> &db_p) : current_level(LogLevel::DISABLED) {
	current_writer = make_shared_ptr<LogWriter>(db_p);
}

void LoggingManager::Log(LogLevel level, const string& logger, const string& log_message, optional_ptr<LoggingContext> context) {
	if (level > current_level) {
		return;
	}
	if (!logger.empty()) {
		unique_lock<mutex> lck(lock);
		if (enabled_loggers.find(logger) == enabled_loggers.end()) {
			return;
		}
		current_writer->Log(level, log_message, context);
	} else {
		unique_lock<mutex> lck(lock);
		current_writer->Log(level, log_message, context);
	}
}
} // namespace duckdb
