#pragma once

#include "duckdb/storage/compression/dictionary/common.hpp"
#include "duckdb/common/string_map_set.hpp"
#include "duckdb/storage/table/column_data.hpp"

namespace duckdb {

//===--------------------------------------------------------------------===//
// Analyze
//===--------------------------------------------------------------------===//
struct DictionaryAnalyzeState : public DictionaryCompressionState {
public:
	explicit DictionaryAnalyzeState(const CompressionInfo &info);

public:
	bool LookupString(string_t str) override;
	void AddNewString(string_t str) override;
	void AddLastLookup() override;
	void AddNull() override;
	bool CalculateSpaceRequirements(bool new_string, idx_t string_size) override;
	void Flush(bool final = false) override;
	void Verify() override;
	void UpdateMaxUniqueCount();

public:
	idx_t segment_count;
	idx_t current_tuple_count;
	idx_t current_unique_count;
	idx_t max_unique_count_across_segments =
	    0; // Is used to allocate the dictionary optimally later on at the InitCompression step
	idx_t current_dict_size;
	StringHeap heap;
	string_set_t current_set;
	bitpacking_width_t current_width;
	bitpacking_width_t next_width;
};

struct DictionaryCompressionAnalyzeState : public AnalyzeState {
public:
	explicit DictionaryCompressionAnalyzeState(const CompressionInfo &info)
	    : AnalyzeState(info), analyze_state(make_uniq<DictionaryAnalyzeState>(info)) {
	}

public:
	unique_ptr<DictionaryAnalyzeState> analyze_state;
};

} // namespace duckdb
