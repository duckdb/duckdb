#pragma once

#include "duckdb/storage/compression/dictionary/common.hpp"
#include "duckdb/common/string_map_set.hpp"
#include "duckdb/storage/table/column_data.hpp"

namespace duckdb {

namespace dict_fsst {

//===--------------------------------------------------------------------===//
// Analyze
//===--------------------------------------------------------------------===//
struct DictFSSTAnalyzeState : public DictFSSTCompressionState {
public:
	explicit DictFSSTAnalyzeState(const CompressionInfo &info);

public:
	bool LookupString(string_t str) override;
	void AddNewString(string_t str) override;
	void AddLastLookup() override;
	void AddNull() override;
	bool CalculateSpaceRequirements(bool new_string, idx_t string_size) override;
	void Flush(bool final = false) override;
	void Verify() override;

public:
	idx_t segment_count;
	idx_t current_tuple_count;
	idx_t current_unique_count;
	idx_t current_dict_size;
	StringHeap heap;
	string_set_t current_set;
	bitpacking_width_t current_width;
	bitpacking_width_t next_width;
};

struct DictFSSTCompressionAnalyzeState : public AnalyzeState {
public:
	explicit DictFSSTCompressionAnalyzeState(const CompressionInfo &info)
	    : AnalyzeState(info), analyze_state(make_uniq<DictFSSTAnalyzeState>(info)) {
	}

public:
	unique_ptr<DictFSSTAnalyzeState> analyze_state;
};

} // namespace dict_fsst

} // namespace duckdb
