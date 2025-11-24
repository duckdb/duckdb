#include "duckdb/main/attached_database.hpp"
#include "duckdb/logging/file_system_logger.hpp"
#include "duckdb/logging/log_type.hpp"
#include "duckdb/common/file_opener.hpp"
#include "duckdb/common/http_util.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/main/database.hpp"
#include "duckdb/execution/physical_operator.hpp"

namespace duckdb {

constexpr LogLevel DefaultLogType::LEVEL;
constexpr LogLevel FileSystemLogType::LEVEL;
constexpr LogLevel QueryLogType::LEVEL;
constexpr LogLevel HTTPLogType::LEVEL;
constexpr LogLevel PhysicalOperatorLogType::LEVEL;
constexpr LogLevel MetricsLogType::LEVEL;
constexpr LogLevel CheckpointLogType::LEVEL;

//===--------------------------------------------------------------------===//
// QueryLogType
//===--------------------------------------------------------------------===//
string QueryLogType::ConstructLogMessage(const string &str) {
	return str;
}

//===--------------------------------------------------------------------===//
// FileSystemLogType
//===--------------------------------------------------------------------===//
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

//===--------------------------------------------------------------------===//
// HTTPLogType
//===--------------------------------------------------------------------===//
HTTPLogType::HTTPLogType() : LogType(NAME, LEVEL, GetLogType()) {
}

LogicalType HTTPLogType::GetLogType() {
	child_list_t<LogicalType> request_child_list = {
	    {"type", LogicalType::VARCHAR},
	    {"url", LogicalType::VARCHAR},
	    {"start_time", LogicalType::TIMESTAMP_TZ},
	    {"duration_ms", LogicalType::BIGINT},
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
	    {"start_time", request.have_request_timing ? Value::TIMESTAMP(request.request_start) : Value()},
	    {"duration_ms", request.have_request_timing ? Value::BIGINT(Timestamp::GetEpochMs(request.request_end) -
	                                                                Timestamp::GetEpochMs(request.request_start))
	                                                : Value()}};
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

//===--------------------------------------------------------------------===//
// PhysicalOperatorLogType
//===--------------------------------------------------------------------===//
PhysicalOperatorLogType::PhysicalOperatorLogType() : LogType(NAME, LEVEL, GetLogType()) {
}

LogicalType PhysicalOperatorLogType::GetLogType() {
	child_list_t<LogicalType> child_list = {
	    {"operator_type", LogicalType::VARCHAR},
	    {"parameters", LogicalType::MAP(LogicalType::VARCHAR, LogicalType::VARCHAR)},
	    {"class", LogicalType::VARCHAR},
	    {"event", LogicalType::VARCHAR},
	    {"info", LogicalType::MAP(LogicalType::VARCHAR, LogicalType::VARCHAR)},
	};
	return LogicalType::STRUCT(child_list);
}

template <class ITERABLE>
static Value StringPairIterableToMap(const ITERABLE &iterable) {
	vector<Value> keys;
	vector<Value> values;
	for (const auto &kv : iterable) {
		keys.emplace_back(kv.first);
		values.emplace_back(kv.second);
	}
	return Value::MAP(LogicalType::VARCHAR, LogicalType::VARCHAR, std::move(keys), std::move(values));
}

string PhysicalOperatorLogType::ConstructLogMessage(const PhysicalOperator &physical_operator, const string &class_p,
                                                    const string &event, const vector<pair<string, string>> &info) {
	child_list_t<Value> child_list = {
	    {"operator_type", EnumUtil::ToString(physical_operator.type)},
	    {"parameters", StringPairIterableToMap(physical_operator.ParamsToString())},
	    {"class", class_p},
	    {"event", event},
	    {"info", StringPairIterableToMap(info)},
	};

	return Value::STRUCT(std::move(child_list)).ToString();
}

//===--------------------------------------------------------------------===//
// MetricsLogType
//===--------------------------------------------------------------------===//
MetricsLogType::MetricsLogType() : LogType(NAME, LEVEL, GetLogType()) {
}

LogicalType MetricsLogType::GetLogType() {
	child_list_t<LogicalType> child_list = {
	    {"metric", LogicalType::VARCHAR},
	    {"value", LogicalType::VARCHAR},
	};
	return LogicalType::STRUCT(child_list);
}

string MetricsLogType::ConstructLogMessage(const MetricsType &metric, const Value &value) {
	child_list_t<Value> child_list = {
	    {"metric", EnumUtil::ToString(metric)},
	    {"value", value.ToString()},
	};
	return Value::STRUCT(std::move(child_list)).ToString();
}

//===--------------------------------------------------------------------===//
// CheckpointLogType
//===--------------------------------------------------------------------===//
CheckpointLogType::CheckpointLogType() : LogType(NAME, LEVEL, GetLogType()) {
}

LogicalType CheckpointLogType::GetLogType() {
	child_list_t<LogicalType> child_list = {
	    {"database", LogicalType::VARCHAR},
	    {"schema", LogicalType::VARCHAR},
	    {"table", LogicalType::VARCHAR},
	    {"type", LogicalType::VARCHAR},
	    {"info", LogicalType::MAP(LogicalType::VARCHAR, LogicalType::VARCHAR)},
	};
	return LogicalType::STRUCT(child_list);
}

string CheckpointLogType::CreateLog(const AttachedDatabase &db, DataTableInfo &table, const char *op_name,
                                    vector<Value> map_keys, vector<Value> map_values) {
	child_list_t<Value> child_list = {
	    {"database", db.name},
	    {"schema", table.GetSchemaName()},
	    {"table", table.GetTableName()},
	    {"type", op_name},
	    {"info", Value::MAP(LogicalType::VARCHAR, LogicalType::VARCHAR, std::move(map_keys), std::move(map_values))},
	};

	return Value::STRUCT(std::move(child_list)).ToString();
}

string CheckpointLogType::ConstructLogMessage(const AttachedDatabase &db, DataTableInfo &table, idx_t segment_idx,
                                              idx_t merge_count, idx_t target_count, idx_t merge_rows,
                                              idx_t row_start) {
	vector<Value> map_keys = {"segment_idx", "merge_count", "target_count", "merge_rows", "row_start"};
	vector<Value> map_values = {to_string(segment_idx), to_string(merge_count), to_string(target_count),
	                            to_string(merge_rows), to_string(row_start)};
	return CreateLog(db, table, "vacuum", std::move(map_keys), std::move(map_values));
}

string CheckpointLogType::ConstructLogMessage(const AttachedDatabase &db, DataTableInfo &table, idx_t segment_idx,
                                              RowGroup &row_group, idx_t row_group_start) {
	vector<Value> map_keys = {"segment_idx", "start", "count"};
	vector<Value> map_values = {to_string(segment_idx), to_string(row_group_start), to_string(row_group.count.load())};
	return CreateLog(db, table, "checkpoint", std::move(map_keys), std::move(map_values));
}

} // namespace duckdb
