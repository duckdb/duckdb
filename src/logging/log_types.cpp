#include "duckdb/logging/file_system_logger.hpp"
#include "duckdb/logging/log_type.hpp"
#include "duckdb/common/file_opener.hpp"
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
		{"fs", LogicalType::VARCHAR},
		{"path", LogicalType::VARCHAR},
		{"op", LogicalType::VARCHAR},
		{"bytes", LogicalType::BIGINT},
		{"pos", LogicalType::BIGINT},
	};
	return LogicalType::STRUCT(child_list);
}
}