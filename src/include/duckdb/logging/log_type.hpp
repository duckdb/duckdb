//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/logging/log_type.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/logging/logging.hpp"
#include "duckdb/common/string_util.hpp"

namespace duckdb {

struct FileHandle;
struct BaseRequest;
struct HTTPResponse;
class PhysicalOperator;

//! Log types provide some structure to the formats that the different log messages can have
//! For now, this holds a type that the VARCHAR value will be auto-cast into.
class LogType {
public:
	//! Construct an unstructured type
	LogType(const string &name_p, const LogLevel &level_p)
	    : name(name_p), level(level_p), is_structured(false), type(LogicalType::VARCHAR) {
	}
	//! Construct a structured type
	LogType(const string &name_p, const LogLevel &level_p, LogicalType structured_type)
	    : name(name_p), level(level_p), is_structured(true), type(std::move(structured_type)) {
	}

	string name;
	LogLevel level;

	bool is_structured;
	LogicalType type;
};

class DefaultLogType : public LogType {
public:
	static constexpr const char *NAME = "";
	static constexpr LogLevel LEVEL = LogLevel::LOG_INFO;

	DefaultLogType() : LogType(NAME, LEVEL) {
	}
};

class QueryLogType : public LogType {
public:
	static constexpr const char *NAME = "QueryLog";
	static constexpr LogLevel LEVEL = LogLevel::LOG_INFO;

	QueryLogType() : LogType(NAME, LEVEL) {};

	static string ConstructLogMessage(const string &str) {
		return str;
	}
};

class FileSystemLogType : public LogType {
public:
	static constexpr const char *NAME = "FileSystem";
	static constexpr LogLevel LEVEL = LogLevel::LOG_TRACE;

	//! Construct the log type
	FileSystemLogType();

	static LogicalType GetLogType();

	static string ConstructLogMessage(const FileHandle &handle, const string &op, int64_t bytes, idx_t pos);
	static string ConstructLogMessage(const FileHandle &handle, const string &op);
};

class HTTPLogType : public LogType {
public:
	static constexpr const char *NAME = "HTTP";
	static constexpr LogLevel LEVEL = LogLevel::LOG_DEBUG;

	//! Construct the log types
	HTTPLogType();

	static LogicalType GetLogType();

	static string ConstructLogMessage(BaseRequest &request, optional_ptr<HTTPResponse> response);

	// FIXME: HTTPLogType should be structured probably
	static string ConstructLogMessage(const string &str) {
		return str;
	}
};

class PhysicalOperatorLogType : public LogType {
public:
	static constexpr const char *NAME = "PhysicalOperator";
	static constexpr LogLevel LEVEL = LogLevel::LOG_DEBUG;

	//! Construct the log type
	PhysicalOperatorLogType();

	static LogicalType GetLogType();

	static string ConstructLogMessage(const PhysicalOperator &op, const string &class_p, const string &event,
	                                  const vector<pair<string, string>> &info);
};

} // namespace duckdb
