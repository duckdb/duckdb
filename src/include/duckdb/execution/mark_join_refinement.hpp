//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/execution/mark_join_refinement.hpp
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/types/column/column_data_collection.hpp"
#include "duckdb/common/map.hpp"
#include <functional>
#include "duckdb/common/set.hpp"
#include "duckdb/planner/joinside.hpp"
#include "duckdb/common/array.hpp"
#include "duckdb/execution/operator/join/physical_range_join.hpp"

namespace duckdb {

class JoinHashTable;
struct IEJoinBuildOrders;
struct MarkJoinRefinementGroup;
using mark_key_fetch_t = std::function<DataChunk &(idx_t)>;

struct MarkPatternClassification {
	bool reducible = false;
	uint64_t dropped = 0;
	uint64_t equality_mask = 0;
	idx_t applicable_count = 0;
	vector<idx_t> ranges;
};

struct MarkJoinRefinementIndex {
	MarkJoinRefinementIndex();
	~MarkJoinRefinementIndex();
	static unique_ptr<MarkJoinRefinementIndex> BuildHash(ClientContext &context, const PhysicalOperator &op,
	                                                     MarkJoinRefinementGroup &group, uint64_t equality_mask,
	                                                     const vector<JoinCondition> &conditions,
	                                                     const mark_key_fetch_t &fetch);

	//! Equality lookup and its row-identity payload.
	vector<idx_t> columns;
	vector<idx_t> output_columns;
	vector<JoinCondition> conditions;
	unique_ptr<JoinHashTable> hash;
	//! Two-key range orders and IE whole-pattern results.
	unique_ptr<IEJoinBuildOrders> ranges;
	map<uint64_t, unique_ptr<ColumnDataCollection>> probe_results;
	//! Single-range extremum and its exact-comparison witness.
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
	static MarkPatternClassification Classify(uint64_t probe_mask, uint64_t build_mask,
	                                          const vector<JoinCondition> &conditions);
	void AddChunk(const DataChunk &keys, idx_t chunk, const vector<JoinCondition> &conditions);
	idx_t SizeInBytes() const;

	map<uint64_t, MarkJoinRefinementGroup> groups;
	vector<array<idx_t, 3>> chunks;
};

class MarkPatternRefiner {
public:
	MarkPatternRefiner(ClientContext &context, const PhysicalComparisonJoin &op, MarkJoinRefinement &refinement,
	                   mutex &lock, mark_key_fetch_t fetch, DataChunk &keys, bool matches[], ValidityMask &validity);
	void Refine();

private:
	bool Finish(idx_t probe, uint64_t dropped);
	bool RefineWitness(idx_t id, idx_t probe, uint64_t dropped);
	MarkJoinRefinementIndex &BuildEqualityIndex(MarkJoinRefinementGroup &group, uint64_t equality_mask);
	void ProbeEqualityIndex(MarkJoinRefinementIndex &index, uint64_t probe_mask, uint64_t dropped,
	                        uint64_t equality_mask);
	void RefineRangePattern(MarkJoinRefinementGroup &group, uint64_t probe_mask, uint64_t build_mask,
	                        vector<idx_t> driving);
	bool RefineOneRange(MarkJoinRefinementGroup &group, idx_t probe, uint64_t dropped, idx_t range_column);
	bool RefineExact(MarkJoinRefinementGroup &group, idx_t probe, uint64_t dropped);

	ClientContext &context;
	const PhysicalComparisonJoin &op;
	const vector<JoinCondition> &conditions;
	MarkJoinRefinement &refinement;
	mutex &lock;
	mark_key_fetch_t fetch;
	DataChunk &chunk;
	DataChunk &keys;
	optional_ptr<bool> matches;
	ValidityMask &validity;
	vector<LogicalType> condition_types;
	DataChunk candidates;
	Vector comparison;
	set<pair<uint64_t, uint64_t>> refinement_batches;
};

} // namespace duckdb
