//===----------------------------------------------------------------------===//
//                         DuckDB
//
// src/include/duckdb/logging/weak_logger.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/fstream.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/main/database.hpp"

#include <functional>

namespace duckdb {

// The WeakLogger is a special logger class which holds `weak_ptr`s to either the ClientContext and/or the
// DatabaseInstance. this allows weak loggers to be used from code that only have access to these during initialization
// (for example: `FileHandle`s) NOTE that this is a little bit of a band-aid solution: ideally we properly pass
// `ClientContext`s everywhere and log directly to those.
class WeakLogger : public Logger {
public:
	explicit WeakLogger(ClientContext &context_p)
	    : Logger(context_p.db->GetLogManager()), context(context_p.shared_from_this()),
	      database(context_p.db->shared_from_this()) {
	}
	explicit WeakLogger(DatabaseInstance &db) : Logger(db.GetLogManager()), database(db.shared_from_this()) {
	}

	bool ShouldLog(const char *log_type, LogLevel log_level) override {
		auto context_ptr = context.lock();
		if (context_ptr) {
			return Logger::Get(*context_ptr).ShouldLog(log_type, log_level);
		}
		auto db_ptr = database.lock();
		if (db_ptr) {
			return Logger::Get(*db_ptr).ShouldLog(log_type, log_level);
		}
		return false;
	}

	void WriteLog(const char *log_type, LogLevel log_level, const char *message) override {
		auto context_ptr = context.lock();
		if (context_ptr) {
			return Logger::Get(*context_ptr).WriteLog(log_type, log_level, message);
		}
		auto db_ptr = database.lock();
		if (db_ptr) {
			return Logger::Get(*db_ptr).WriteLog(log_type, log_level, message);
		}
	}

	// Syntactic sugar for formatted strings
	template <typename... ARGS>
	void WriteLog(const char *log_type, LogLevel log_level, const char *format_string, ARGS... params) {
		auto formatted_string = StringUtil::Format(format_string, params...);
		WriteLog(log_type, log_level, formatted_string.c_str());
	}

	void Flush() override {
		auto context_ptr = context.lock();
		if (context_ptr) {
			return Logger::Get(*context_ptr).Flush();
		}
		auto db_ptr = database.lock();
		if (db_ptr) {
			return Logger::Get(*db_ptr).Flush();
		}
	}

	bool IsThreadSafe() override {
		return true;
	}

	const LogConfig &GetConfig() const override {
		auto context_ptr = context.lock();
		if (context_ptr) {
			return Logger::Get(*context_ptr).GetConfig();
		}

		auto db_ptr = database.lock();
		if (db_ptr) {
			return Logger::Get(*db_ptr).GetConfig();
		}

		return default_config;
	}

protected:
	const weak_ptr<ClientContext> context;
	const weak_ptr<DatabaseInstance> database;
	LogConfig default_config;
};

} // namespace duckdb
