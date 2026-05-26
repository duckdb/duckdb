#pragma once

#include "duckdb/storage/compression/dictionary/common.hpp"
#include "duckdb/common/string_map_set.hpp"
#include "duckdb/storage/table/column_data.hpp"

namespace duckdb {

//===--------------------------------------------------------------------===//
// Analyze
//===--------------------------------------------------------------------===//
struct DictionaryAnalyzeState : public AnalyzeState {
public:
	explicit DictionaryAnalyzeState(BlockManager &block_manager);

public:
	bool LookupString(string_t str);
	void AddNewString(string_t str);
	void AddLastLookup();
	void AddNull();
	bool CalculateSpaceRequirements(bool new_string, idx_t string_size);
	void Flush(bool final = false);
	void Verify();
	void UpdateMaxUniqueCount();

public:
	idx_t segment_count;
	idx_t current_tuple_count;
	idx_t current_unique_count;
	//! Used to allocate the dictionary optimally later on at the InitCompression step
	idx_t max_unique_count_across_segments = 0;
	idx_t current_dict_size;
	StringHeap heap;
	string_set_t current_set;
	bitpacking_width_t current_width;
	bitpacking_width_t next_width;
};
} // namespace duckdb
