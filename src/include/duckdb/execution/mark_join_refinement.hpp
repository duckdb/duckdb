//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/execution/mark_join_refinement.hpp
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/types/column/column_data_collection.hpp"
#include "duckdb/common/map.hpp"
#include "duckdb/common/arena_containers/arena_vector.hpp"
#include "duckdb/common/arena_containers/arena_ptr.hpp"
#include "duckdb/common/array.hpp"
#include <functional>
#include "duckdb/common/set.hpp"
#include "duckdb/planner/joinside.hpp"
#include "duckdb/execution/mark_join_row_comparison.hpp"
#include "duckdb/execution/operator/join/physical_range_join.hpp"

namespace duckdb {

class JoinHashTable;
struct IEJoinBuildOrders;
struct MarkJoinRefinementGroup;
using mark_key_fetch_t = std::function<DataChunk &(idx_t)>;
using mark_candidate_finish_t = std::function<void(const SelectionVector &, const SelectionVector &, idx_t, Vector &)>;

struct MarkPatternClassification {
	bool reducible = false;
	uint64_t dropped = 0;
	uint64_t equality_mask = 0;
	idx_t applicable_count = 0;
	vector<idx_t> ranges;
};

template <class KEY, class VALUE>
using mark_refinement_map_t = map<KEY, VALUE, std::less<KEY>, arena_stl_allocator<pair<const KEY, VALUE>>>;

struct MarkJoinRefinementIndex {
	MarkJoinRefinementIndex();
	~MarkJoinRefinementIndex();
	static arena_ptr<MarkJoinRefinementIndex> Create(ArenaAllocator &arena);
	static arena_ptr<MarkJoinRefinementIndex> BuildHash(ClientContext &context, const PhysicalOperator &op,
	                                                    MarkJoinRefinementGroup &group, uint64_t equality_mask,
	                                                    const vector<JoinCondition> &conditions,
	                                                    const mark_key_fetch_t &fetch);

	//! Equality lookup and its row-identity payload.
	vector<idx_t> columns;
	vector<idx_t> output_columns;
	vector<JoinCondition> conditions;
	unique_ptr<JoinHashTable> hash;
	//! Two-key range orders.
	unique_ptr<IEJoinBuildOrders> ranges;
	//! Single-range extremum and its exact-comparison witness.
	idx_t witness = 0;
};

struct MarkJoinRefinementGroup {
	explicit MarkJoinRefinementGroup(ArenaAllocator &arena) : arena(arena), selections(arena), indexes(arena) {
	}
	ArenaAllocator &arena;
	mark_refinement_map_t<idx_t, arena_vector<sel_t>> selections;
	mark_refinement_map_t<uint64_t, arena_ptr<MarkJoinRefinementIndex>> indexes;
	idx_t count = 0;
};

struct MarkJoinRefinement {
	MarkJoinRefinement(ClientContext &context, std::function<void(idx_t, idx_t, bool)> update_memory);
	void Reserve(idx_t additional = 0);
	void BuildIndex(idx_t additional, const std::function<void()> &build);
	static vector<uint64_t> NullMasks(const DataChunk &keys, const vector<JoinCondition> &conditions);
	static MarkPatternClassification Classify(uint64_t probe_mask, uint64_t build_mask,
	                                          const vector<JoinCondition> &conditions);
	void AddChunk(const DataChunk &keys, idx_t chunk, const vector<JoinCondition> &conditions);
	idx_t SizeInBytes() const;

	ArenaAllocator arena;
	mark_refinement_map_t<uint64_t, MarkJoinRefinementGroup> groups;
	arena_vector<array<idx_t, 3>> chunks;
	std::function<void(idx_t, idx_t, bool)> update_memory;
};

class MarkPatternRefiner {
public:
	MarkPatternRefiner(ClientContext &context, const PhysicalComparisonJoin &op, MarkJoinRefinement &refinement,
	                   mutex &lock, mark_key_fetch_t fetch, DataChunk &keys, bool matches[], ValidityMask &validity,
	                   mark_candidate_finish_t finish_candidates = {});
	~MarkPatternRefiner();
	void Refine();

private:
	void Fetch(idx_t index);
	bool Finish(idx_t probe, uint64_t dropped, const SelectionVector &build_selection);
	bool RefineWitness(idx_t id, idx_t probe, uint64_t dropped);
	MarkJoinRefinementIndex &BuildEqualityIndex(MarkJoinRefinementGroup &group, uint64_t equality_mask);
	void ProbeEqualityIndex(MarkJoinRefinementIndex &index, uint64_t probe_mask, uint64_t dropped,
	                        uint64_t equality_mask);
	void RefineRangePattern(MarkJoinRefinementGroup &group, idx_t probe, uint64_t probe_mask, uint64_t build_mask,
	                        const vector<idx_t> &ranges);
	unique_ptr<IEJoinBuildOrders> BuildRangeIndex(ExecutionContext &execution, MarkJoinRefinementGroup &group,
	                                              const vector<idx_t> &driving,
	                                              const vector<JoinCondition> &range_conditions);
	void RunRangeJoin(ExecutionContext &execution, IEJoinBuildOrders &build, const vector<idx_t> &driving,
	                  const vector<JoinCondition> &range_conditions, uint64_t probe_mask, uint64_t dropped,
	                  const mark_key_fetch_t &probe_fetch, idx_t probe_count, AllocatedData &markers);
	void ApplyMarker(idx_t probe, uint8_t marker);
	void RefineOneRange(MarkJoinRefinementGroup &group, uint64_t probe_mask, uint64_t dropped, idx_t range_column);
	void RefineWitnessBatch(idx_t id, uint64_t probe_mask);
	bool RefineExact(MarkJoinRefinementGroup &group, idx_t probe, uint64_t dropped);

	ClientContext &context;
	const PhysicalComparisonJoin &op;
	const vector<JoinCondition> &conditions;
	MarkJoinRefinement &refinement;
	mutex &lock;
	mark_key_fetch_t fetch;
	mark_candidate_finish_t finish_candidates;
	DataChunk &chunk;
	idx_t cached_chunk = 0;
	DataChunk &keys;
	vector<uint64_t> probe_masks;
	optional_ptr<bool> matches;
	ValidityMask &validity;
	vector<LogicalType> condition_types;
	DataChunk candidates;
	Vector comparison;
	MarkJoinRowComparison comparer;
	set<pair<uint64_t, uint64_t>> refinement_batches;
};

} // namespace duckdb
