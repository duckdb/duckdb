//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/logging/file_handle_logger.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/file_opener.hpp"
#include "duckdb/common/fstream.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/logging/logger.hpp"

#include <functional>

namespace duckdb {

// Can't use the regular filehandles here because the logger might not always be set
#define DUCKDB_LOG_FILE_HANDLE_BYTES(HANDLE, OP, BYTES, POS)                                                           \
	{                                                                                                                  \
		if (HANDLE.logger && HANDLE.logger->ShouldLog(FileHandleLogger::LOG_TYPE, FileHandleLogger::LOG_LEVEL)) {      \
			auto string = FileHandleLogger::ConstructLogMessage(HANDLE, OP, BYTES, POS);                               \
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

// Macros for logging to file handles
#define DUCKDB_LOG_FILE_HANDLE_READ(HANDLE, BYTES, POS)  DUCKDB_LOG_FILE_HANDLE_BYTES(HANDLE, "READ", BYTES, POS);
#define DUCKDB_LOG_FILE_HANDLE_WRITE(HANDLE, BYTES, POS) DUCKDB_LOG_FILE_HANDLE_BYTES(HANDLE, "WRITE", BYTES, POS);
#define DUCKDB_LOG_FILE_HANDLE_OPEN(HANDLE)              DUCKDB_LOG_FILE_HANDLE(HANDLE, "OPEN");
#define DUCKDB_LOG_FILE_HANDLE_CLOSE(HANDLE)             DUCKDB_LOG_FILE_HANDLE(HANDLE, "CLOSE");

class FileHandleLogger {
public:
	//! The log type to use for logging to filehandles
	static constexpr const char *LOG_TYPE = "duckdb.FileHandle";
	//! The log type to use for logging to filehandles
	static constexpr LogLevel LOG_LEVEL = LogLevel::LOG_TRACE;

	//! This copies the logger pointer if necessary. Note that we only copy
	DUCKDB_API static shared_ptr<Logger> CopyLoggerPtr(FileOpener &opener) {
		auto context = opener.TryGetClientContext();
		if (context && Logger::Get(*context).ShouldLog(LOG_TYPE, LOG_LEVEL)) {
			return context->logger;
		}
		auto database = opener.TryGetDatabase();
		if (database && Logger::Get(*database).ShouldLog(LOG_TYPE, LOG_LEVEL)) {
			return database->GetLogManager().CopyGlobalLoggerPtr();
		}
		return nullptr;
	}

	// FIXME: Manual JSON strings are not winning any style points
	static string ConstructLogMessage(const FileHandle &handle, const string &op, int64_t bytes, idx_t pos) {
		return StringUtil::Format("{\"fs\":\"%s\",\"path\":\"%s\",\"op\":\"%s\",\"bytes\":\"%lld\",\"pos\":\"%llu\"}",
		                          handle.file_system.GetName(), handle.path, op, bytes, pos);
	}
	static string ConstructLogMessage(const FileHandle &handle, const string &op) {
		return StringUtil::Format("{\"fs\":\"%s\",\"path\":\"%s\",\"op\":\"%s\"}", handle.file_system.GetName(),
		                          handle.path, op);
	}
};

} // namespace duckdb
