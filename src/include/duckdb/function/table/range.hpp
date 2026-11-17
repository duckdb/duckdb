//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/function/table/range.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/function/table_function.hpp"
#include "duckdb/function/built_in_functions.hpp"
#include "duckdb/common/enums/file_glob_options.hpp"
#include "duckdb/common/open_file_info.hpp"

namespace duckdb {

struct CheckpointFunction {
	static void RegisterFunction(BuiltinFunctions &set);
};

class MultiFileList;

struct GlobTableFunction {
	static void RegisterFunction(BuiltinFunctions &set);
	//! The glob table function - lists the files that match a glob (or a list of globs)
	DUCKDB_API static TableFunction GetFunction();
	//! A list of the files the given globs match, expanded lazily by running the glob table function. Entries that
	//! carry explicit open options (an ExtendedOpenFileInfo) are not globbed but listed as-is
	DUCKDB_API static unique_ptr<MultiFileList> CreateFileList(ClientContext &context, vector<OpenFileInfo> globs,
	                                                           const FileGlobInput &glob_input);
};

struct RangeTableFunction {
	static void RegisterFunction(BuiltinFunctions &set);
};

struct RepeatTableFunction {
	static void RegisterFunction(BuiltinFunctions &set);
};

struct RepeatRowTableFunction {
	static void RegisterFunction(BuiltinFunctions &set);
};

struct UnnestTableFunction {
	static void RegisterFunction(BuiltinFunctions &set);
};

struct CSVSnifferFunction {
	static void RegisterFunction(BuiltinFunctions &set);
};

struct ReadBlobFunction {
	static void RegisterFunction(BuiltinFunctions &set);
};

struct ReadTextFunction {
	static void RegisterFunction(BuiltinFunctions &set);
};

struct QueryTableFunction {
	static void RegisterFunction(BuiltinFunctions &set);
};

} // namespace duckdb
