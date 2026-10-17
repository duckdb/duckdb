//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/process_util.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/constants.hpp"

namespace duckdb {
class FileSystem;

//! What the operating system can tell us about processes: who we are, who is still alive, and
//! who to name in an error message.
struct ProcessUtil {
	//! The id of the process this code is running in
	DUCKDB_API static int64_t CurrentProcessId();
	//! Whether a process with this id exists. Answers true whenever it cannot tell, so a process
	//! we may not inspect is never taken for a dead one.
	DUCKDB_API static bool ProcessIsRunning(int64_t pid);
	//! Best-effort description of a process - its name, and its owner where we can get it - for
	//! error messages. Empty when the platform cannot say.
	DUCKDB_API static string GetProcessDescription(FileSystem &fs, int64_t pid);
};

} // namespace duckdb
