//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/execution/mark_join_refinement.hpp
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/types/column/column_data_collection.hpp"
#include "duckdb/common/map.hpp"
#include "duckdb/planner/joinside.hpp"
#include "duckdb/common/array.hpp"
#include "duckdb/execution/operator/join/physical_range_join.hpp"

namespace duckdb {

class JoinHashTable;
struct IEJoinBuildOrders;

struct MarkJoinRefinementIndex {
	MarkJoinRefinementIndex();
	~MarkJoinRefinementIndex();
	vector<idx_t> columns;
	vector<idx_t> output_columns;
	vector<JoinCondition> conditions;
	unique_ptr<JoinHashTable> hash;
	unique_ptr<IEJoinBuildOrders> ranges;
	map<uint64_t, unique_ptr<ColumnDataCollection>> probe_results;
	Value bound;
	idx_t witness = 0;
};

struct MarkJoinRefinementGroup {
	map<idx_t, vector<sel_t>> selections;
	map<uint64_t, unique_ptr<MarkJoinRefinementIndex>> indexes;
	idx_t count = 0;
};

struct MarkJoinRefinement {
	static uint64_t NullMask(const DataChunk &keys, idx_t row, const vector<JoinCondition> &conditions);
	void AddChunk(const DataChunk &keys, idx_t chunk, const vector<JoinCondition> &conditions);
	idx_t SizeInBytes() const;

	map<uint64_t, MarkJoinRefinementGroup> groups;
	vector<array<idx_t, 3>> chunks;
};

} // namespace duckdb
