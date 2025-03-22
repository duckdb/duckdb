#pragma once

#include "duckdb/storage/compression/dict_fsst/common.hpp"
#include "duckdb/common/string_map_set.hpp"
#include "duckdb/storage/table/column_data.hpp"

namespace duckdb {

namespace dict_fsst {

//===--------------------------------------------------------------------===//
// Analyze
//===--------------------------------------------------------------------===//
struct DictFSSTAnalyzeState : public AnalyzeState {
public:
	explicit DictFSSTAnalyzeState(const CompressionInfo &info);

public:
	bool Analyze(Vector &input, idx_t count);
	idx_t FinalAnalyze();

public:
	idx_t max_string_length = 0;
	bool contains_nulls = false;
	idx_t total_string_length = 0;
	idx_t total_count = 0;
};

} // namespace dict_fsst

} // namespace duckdb
