#pragma once

#include "duckdb/common/types/hyperloglog.hpp"
#include "duckdb/common/unordered_set.hpp"
#include "duckdb/function/compression_function.hpp"

namespace duckdb {

namespace dict_fsst {

//===--------------------------------------------------------------------===//
// Analyze
//===--------------------------------------------------------------------===//
struct DictFSSTAnalyzeState : public AnalyzeState {
public:
	explicit DictFSSTAnalyzeState(BlockManager &block_manager);

public:
	bool Analyze(const Vector &input);
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
	idx_t non_null_count = 0;
	//! Approximate number of distinct strings: sizes the dictionary and the width of the dictionary indices
	HyperLogLogP<10> distinct_strings;
	//! Exact bytes of the first EXACT_DISTINCT_LIMIT distinct strings, so that a few long repeated values next to many
	//! short ones do not get scaled as if every string had the average length
	static constexpr idx_t EXACT_DISTINCT_LIMIT = 2048;
	unordered_set<hash_t> exact_distinct;
	idx_t exact_distinct_length = 0;
	//! Strings that arrived after the exact set filled up and are not in it: their distinct bytes are scaled
	idx_t scaled_string_length = 0;
	idx_t scaled_count = 0;
};

} // namespace dict_fsst

} // namespace duckdb
