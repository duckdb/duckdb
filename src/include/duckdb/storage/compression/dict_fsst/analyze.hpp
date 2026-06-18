#pragma once

#include "duckdb/storage/compression/dict_fsst/common.hpp"
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
	//! Effective exclusive size limit for plain dictionary encoding.
	idx_t string_size_limit = 0;
	//! Effective exclusive input size limit for worst-case FSST encoding.
	idx_t fsst_string_size_limit = 0;
	//! Flag which disables the usage of FSST if worst-case encoding blowup does not fit the block size.
	bool disable_fsst = false;
	idx_t total_string_length = 0;
	idx_t total_count = 0;
};

} // namespace dict_fsst

} // namespace duckdb
