//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/logging/log_storage.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/atomic.hpp"
#include "duckdb/common/mutex.hpp"
#include "duckdb/common/optional_idx.hpp"
#include "duckdb/common/types.hpp"
#include "duckdb/common/unordered_set.hpp"

#include <duckdb/parallel/thread_context.hpp>

namespace duckdb {

// TODO: should we reconsider these logging levels?
enum class LogLevel : uint8_t {
	DEBUGGING = 10,
	INFO = 20,
	WARN = 30,
	ERROR = 40,
	FATAL = 50,
};

enum class LogContextScope : uint8_t {
	DATABASE = 10,
	CONNECTION = 20,
	THREAD = 30,
};

enum class LogMode : uint8_t {
	LEVEL_ONLY = 0,
	DISABLE_SELECTED = 1,
	ENABLE_SELECTED = 2,
};

enum class LogDestinationType : uint8_t {
	IN_MEMORY = 0,
};

struct LogConfig {
	LogConfig();

	static LogConfig Create(bool enabled, LogLevel level);
	static LogConfig CreateFromEnabled(bool enabled, LogLevel level, unordered_set<string> &enabled_loggers);
	static LogConfig CreateFromDisabled(bool enabled, LogLevel level, unordered_set<string> &disabled_loggers);

	bool IsConsistent() const;

	bool enabled;
	LogMode mode;
	LogLevel level;
	LogDestinationType output;
	unordered_set<string> enabled_loggers;
	unordered_set<string> disabled_loggers;

protected:
	LogConfig(bool enabled, LogLevel level, LogMode mode, optional_ptr<unordered_set<string>> enabled_loggers,
		  optional_ptr<unordered_set<string>> disable_loggers);
};

struct LoggingContext {
	explicit LoggingContext(LogContextScope scope_p) : scope(scope_p){
	}

	LogContextScope scope;

	// TODO: potentially we might want to add stuff to identify which connection and thread the log came from
	optional_idx thread;
	optional_idx client_context;
	optional_idx transaction_id;

	const char *default_log_type = "default";
};

struct RegisteredLoggingContext {
	idx_t context_id;
	LoggingContext context;
};

}