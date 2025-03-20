//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/logging/file_handle_logger.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/fstream.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/logging/weak_logger.hpp"
#include "duckdb/logging/logger.hpp"

#include <functional>

namespace duckdb {

// Syntactic sugar around the http handle logger
#define DUCKDB_LOG_FILE_HANDLE_VA(HANDLE, OP, ...)                                                                     \
	{                                                                                                                  \
		if (HANDLE.logger && HANDLE.logger->ShouldLog(FileHandleLogger::LOG_TYPE, FileHandleLogger::LOG_LEVEL)) {      \
			auto string = FileHandleLogger::ConstructLogMessage(HANDLE, OP, __VA_ARGS__);                              \
			HANDLE.logger->WriteLog(FileHandleLogger::LOG_TYPE, FileHandleLogger::LOG_LEVEL, string.c_str());          \
		}                                                                                                              \
	}
#define DUCKDB_LOG_FILE_HANDLE(HANDLE, OP)                                                                             \
	{                                                                                                                  \
		if (HANDLE.logger && HANDLE.logger->ShouldLog(FileHandleLogger::LOG_TYPE, FileHandleLogger::LOG_LEVEL)) {      \
			auto string = FileHandleLogger::ConstructLogMessage(HANDLE, OP);                                           \
			HANDLE.logger->WriteLog(FileHandleLogger::LOG_TYPE, FileHandleLogger::LOG_LEVEL, string.c_str());          \
		}                                                                                                              \
	}

#define DUCKDB_LOG_FILE_HANDLE_READ(HANDLE, ...)  DUCKDB_LOG_FILE_HANDLE_VA(HANDLE, "READ", __VA_ARGS__);
#define DUCKDB_LOG_FILE_HANDLE_WRITE(HANDLE, ...) DUCKDB_LOG_FILE_HANDLE_VA(HANDLE, "WRITE", __VA_ARGS__);
#define DUCKDB_LOG_FILE_HANDLE_OPEN(HANDLE)       DUCKDB_LOG_FILE_HANDLE(HANDLE, "OPEN");
#define DUCKDB_LOG_FILE_HANDLE_CLOSE(HANDLE)      DUCKDB_LOG_FILE_HANDLE(HANDLE, "CLOSE");

class FileHandleLogger : public WeakLogger {
public:
	static constexpr const char *LOG_TYPE = "duckdb.FileHandle";
	static constexpr LogLevel LOG_LEVEL = LogLevel::LOG_TRACE;

	explicit FileHandleLogger(ClientContext &context) : WeakLogger(context) {
	}
	explicit FileHandleLogger(DatabaseInstance &db) : WeakLogger(db) {
	}

	static unique_ptr<FileHandleLogger> TryCreateFileHandleLogger(FileOpener &opener) {
		auto context = opener.TryGetClientContext();
		if (context && Logger::Get(*context).ShouldLog(LOG_TYPE, LOG_LEVEL)) {
			return make_uniq<FileHandleLogger>(*context);
		}
		auto db = opener.TryGetDatabase();
		if (db && Get(*db).ShouldLog(LOG_TYPE, LOG_LEVEL)) {
			return make_uniq<FileHandleLogger>(*db);
		}
		return nullptr;
	}

	// FIXME: Manual JSON strings are not winning any style points
	static string ConstructLogMessage(const FileHandle &handle, const string &op, int64_t bytes, idx_t pos) {
		return StringUtil::Format("{\"fs\":\"%s\",\"path\":\"%s\",\"op\":\"%s\",\"bytes\":\"%lld\",\"pos\":\"%llu\"}",
		                          handle.file_system.GetName(), handle.path, op, bytes, pos);
	}
	static string ConstructLogMessage(const FileHandle &handle, const string &op, int64_t bytes) {
		return StringUtil::Format("{\"fs\":\"%s\",\"path\":\"%s\",\"op\":\"%s\",\"bytes\":\"%lld\"}",
		                          handle.file_system.GetName(), handle.path, op, bytes);
	}
	static string ConstructLogMessage(const FileHandle &handle, const string &op) {
		return StringUtil::Format("{\"fs\":\"%s\",\"path\":\"%s\",\"op\":\"%s\"}", handle.file_system.GetName(),
		                          handle.path, op);
	}
};

} // namespace duckdb
