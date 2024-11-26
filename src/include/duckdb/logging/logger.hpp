//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/logging/logger.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/main/database.hpp"
#include "duckdb/common/types.hpp"

namespace duckdb {
class TableDescription;

enum class LogLevel : uint8_t {
	DISABLED = 0,
	DEBUGGING = 10,
	INFORMATIVE = 20,
	WARNING = 30,
	ERROR = 40,
	FATAL = 50,
};

struct LoggingContext {
	// TODO: potentially we might want to add stuff to identify which connection and thread the log came from
	idx_t thread_id;
	idx_t connection_id;
};

// Not thread-safe!
class LogWriter {
public:
	LogWriter(shared_ptr<DatabaseInstance> &db);
	void Log(LogLevel level, const string& log_message, optional_ptr<LoggingContext> context);
	void Flush();

private:
	unique_ptr<TableDescription> description;
	DataChunk buffer;
	idx_t buffer_size;
	idx_t max_buffer_size;
	weak_ptr<DatabaseInstance> db;
};

//! Class that handles registering
class LoggingManager {
public:
	LoggingManager(shared_ptr<DatabaseInstance> &db_p);
	void Log(LogLevel level, const string& logger, const string& log_message, optional_ptr<LoggingContext> context);
	void SetLogLevel(LogLevel new_level) {
		current_level = new_level;
	}
	void EnableLogger(const string& logger) {
		unique_lock<mutex> lck(lock);
		enabled_loggers.insert(logger);
	}
	void DisableLogger(const string& logger) {
		unique_lock<mutex> lck(lock);
		enabled_loggers.erase(logger);
	}
	void Flush() {
		unique_lock<mutex> lck(lock);
		current_writer->Flush();
	}

private:
	atomic<LogLevel> current_level;

	mutex lock;
	unordered_set<string> enabled_loggers;
	shared_ptr<LogWriter> current_writer;
};

} // namespace duckdb
