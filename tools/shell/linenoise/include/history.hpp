//===----------------------------------------------------------------------===//
//                         DuckDB
//
// history.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/common.hpp"

namespace duckdb {

class History {
public:
	static void Free();
	static void Clear();
	static idx_t GetLength();
	static const char *GetEntry(idx_t index);
	static void Overwrite(idx_t index, const char *new_entry);
	static void RemoveLastEntry();
	static int Add(const char *line);
	static int Add(const char *line, idx_t len);
	static int SetMaxLength(idx_t len);
	static int Save(const char *filename);
	static int Load(const char *filename);
	//! Load history treating each line as a separate entry (no SQL multi-line joining)
	static int LoadRaw(const char *filename);
};

} // namespace duckdb
