#include "duckdb/logging/file_system_logger.hpp"
#include "duckdb/logging/log_type.hpp"
#include "duckdb/common/file_opener.hpp"
#include "duckdb/common/http_util.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/main/database.hpp"

namespace duckdb {

constexpr LogLevel DefaultLogType::LEVEL;
constexpr LogLevel FileSystemLogType::LEVEL;
constexpr LogLevel QueryLogType::LEVEL;
constexpr LogLevel HTTPLogType::LEVEL;

FileSystemLogType::FileSystemLogType() : LogType(NAME, LEVEL, GetLogType()) {
}

// FIXME: Manual JSON strings are not winning any style points
string FileSystemLogType::ConstructLogMessage(const FileHandle &handle, const string &op, int64_t bytes, idx_t pos) {
	return StringUtil::Format("{\"fs\":\"%s\",\"path\":\"%s\",\"op\":\"%s\",\"bytes\":\"%lld\",\"pos\":\"%llu\"}",
	                          handle.file_system.GetName(), handle.path, op, bytes, pos);
}
string FileSystemLogType::ConstructLogMessage(const FileHandle &handle, const string &op) {
	return StringUtil::Format("{\"fs\":\"%s\",\"path\":\"%s\",\"op\":\"%s\"}", handle.file_system.GetName(),
	                          handle.path, op);
}

LogicalType FileSystemLogType::GetLogType() {
	LogicalType result;
	child_list_t<LogicalType> child_list = {
	    {"fs", LogicalType::VARCHAR},   {"path", LogicalType::VARCHAR}, {"op", LogicalType::VARCHAR},
	    {"bytes", LogicalType::BIGINT}, {"pos", LogicalType::BIGINT},
	};
	return LogicalType::STRUCT(child_list);
}

HTTPLogType::HTTPLogType() : LogType(NAME, LEVEL, GetLogType()) {
}

LogicalType HTTPLogType::GetLogType() {
	child_list_t<LogicalType> request_child_list = {
	    {"type", LogicalType::VARCHAR},
	    {"url", LogicalType::VARCHAR},
	    {"headers", LogicalType::MAP(LogicalType::VARCHAR, LogicalType::VARCHAR)},
	};
	auto request_type = LogicalType::STRUCT(request_child_list);

	child_list_t<LogicalType> response_child_list = {
	    {"status", LogicalType::VARCHAR},
	    {"reason", LogicalType::VARCHAR},
	    {"headers", LogicalType::MAP(LogicalType::VARCHAR, LogicalType::VARCHAR)},
	};
	auto response_type = LogicalType::STRUCT(response_child_list);
	;

	LogicalType result_type;
	child_list_t<LogicalType> child_list = {{"request", request_type}, {"response", response_type}};
	return LogicalType::STRUCT(child_list);
}

static Value CreateHTTPHeadersValue(const HTTPHeaders &headers) {
	vector<Value> keys;
	vector<Value> values;
	for (const auto &header : headers) {
		keys.emplace_back(header.first);
		values.emplace_back(header.second);
	}
	return Value::MAP(LogicalType::VARCHAR, LogicalType::VARCHAR, keys, values);
}

string HTTPLogType::ConstructLogMessage(BaseRequest &request, optional_ptr<HTTPResponse> response) {
	child_list_t<Value> request_child_list = {
	    {"type", Value(EnumUtil::ToString(request.type))},
	    {"url", Value(request.url)},
	    {"headers", CreateHTTPHeadersValue(request.headers)},
	};
	auto request_value = Value::STRUCT(request_child_list);
	Value response_value;
	if (response) {
		child_list_t<Value> response_child_list = {
		    {"status", Value(EnumUtil::ToString(response->status))},
		    {"reason", Value(response->reason)},
		    {"headers", CreateHTTPHeadersValue(response->headers)},
		};
		response_value = Value::STRUCT(response_child_list);
	}

	child_list_t<Value> child_list = {{"request", request_value}, {"response", response_value}};

	return Value::STRUCT(child_list).ToString();
}

} // namespace duckdb
