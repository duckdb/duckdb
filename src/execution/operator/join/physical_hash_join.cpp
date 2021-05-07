#include "duckdb/execution/operator/join/physical_hash_join.hpp"

#include "duckdb/common/vector_operations/vector_operations.hpp"
#include "duckdb/execution/expression_executor.hpp"
#include "duckdb/function/aggregate/distributive_functions.hpp"
#include "duckdb/storage/buffer_manager.hpp"
#include "duckdb/storage/storage_manager.hpp"

namespace duckdb {

PhysicalHashJoin::PhysicalHashJoin(LogicalOperator &op, unique_ptr<PhysicalOperator> left,
                                   unique_ptr<PhysicalOperator> right, vector<JoinCondition> cond, JoinType join_type,
                                   const vector<idx_t> &left_projection_map,
                                   const vector<idx_t> &right_projection_map_p, vector<LogicalType> delim_types,
                                   idx_t estimated_cardinality, PerfectHashJoinState join_state)
    : PhysicalComparisonJoin(op, PhysicalOperatorType::HASH_JOIN, move(cond), join_type, estimated_cardinality),
      right_projection_map(right_projection_map_p), delim_types(move(delim_types)), perfect_join_state(join_state) {
	children.push_back(move(left));
	children.push_back(move(right));

	D_ASSERT(left_projection_map.size() == 0);
	for (auto &condition : conditions) {
		condition_types.push_back(condition.left->return_type);
	}

	// for ANTI, SEMI and MARK join, we only need to store the keys, so for these the build types are empty
	if (join_type != JoinType::ANTI && join_type != JoinType::SEMI && join_type != JoinType::MARK) {
		build_types = LogicalOperator::MapTypes(children[1]->GetTypes(), right_projection_map);
	}
}

PhysicalHashJoin::PhysicalHashJoin(LogicalOperator &op, unique_ptr<PhysicalOperator> left,
                                   unique_ptr<PhysicalOperator> right, vector<JoinCondition> cond, JoinType join_type,
                                   idx_t estimated_cardinality, PerfectHashJoinState perfect_join_state)
    : PhysicalHashJoin(op, move(left), move(right), move(cond), join_type, {}, {}, {}, estimated_cardinality,
                       perfect_join_state) {
}

//===--------------------------------------------------------------------===//
// Sink
//===--------------------------------------------------------------------===//
class HashJoinLocalState : public LocalSinkState {
public:
	DataChunk build_chunk;
	DataChunk join_keys;
	ExpressionExecutor build_executor;
};

class HashJoinGlobalState : public GlobalOperatorState {
public:
	HashJoinGlobalState() {
	}

	//! The HT used by the join
	unique_ptr<JoinHashTable> hash_table;
	//! Only used for FULL OUTER JOIN: scan state of the final scan to find unmatched tuples in the build-side
	JoinHTScanState ht_scan_state;
};

unique_ptr<GlobalOperatorState> PhysicalHashJoin::GetGlobalState(ClientContext &context) {
	auto state = make_unique<HashJoinGlobalState>();
	state->hash_table =
	    make_unique<JoinHashTable>(BufferManager::GetBufferManager(context), conditions, build_types, join_type);
	if (!delim_types.empty() && join_type == JoinType::MARK) {
		// correlated MARK join
		if (delim_types.size() + 1 == conditions.size()) {
			// the correlated MARK join has one more condition than the amount of correlated columns
			// this is the case in a correlated ANY() expression
			// in this case we need to keep track of additional entries, namely:
			// - (1) the total amount of elements per group
			// - (2) the amount of non-null elements per group
			// we need these to correctly deal with the cases of either:
			// - (1) the group being empty [in which case the result is always false, even if the comparison is NULL]
			// - (2) the group containing a NULL value [in which case FALSE becomes NULL]
			auto &info = state->hash_table->correlated_mark_join_info;

			vector<LogicalType> payload_types;
			vector<BoundAggregateExpression *> correlated_aggregates;
			unique_ptr<BoundAggregateExpression> aggr;

			// jury-rigging the GroupedAggregateHashTable
			// we need a count_star and a count to get counts with and without NULLs
			aggr = AggregateFunction::BindAggregateFunction(context, CountStarFun::GetFunction(), {}, nullptr, false);
			correlated_aggregates.push_back(&*aggr);
			payload_types.push_back(aggr->return_type);
			info.correlated_aggregates.push_back(move(aggr));

			auto count_fun = CountFun::GetFunction();
			vector<unique_ptr<Expression>> children;
			// this is a dummy but we need it to make the hash table understand whats going on
			children.push_back(make_unique_base<Expression, BoundReferenceExpression>(count_fun.return_type, 0));
			aggr = AggregateFunction::BindAggregateFunction(context, count_fun, move(children), nullptr, false);
			correlated_aggregates.push_back(&*aggr);
			payload_types.push_back(aggr->return_type);
			info.correlated_aggregates.push_back(move(aggr));

			info.correlated_counts = make_unique<GroupedAggregateHashTable>(
			    BufferManager::GetBufferManager(context), delim_types, payload_types, correlated_aggregates);
			info.correlated_types = delim_types;
			// FIXME: these can be initialized "empty" (without allocating empty vectors)
			info.group_chunk.Initialize(delim_types);
			info.payload_chunk.Initialize(payload_types);
			info.result_chunk.Initialize(payload_types);
		}
	}
	return move(state);
}

unique_ptr<LocalSinkState> PhysicalHashJoin::GetLocalSinkState(ExecutionContext &context) {
	auto state = make_unique<HashJoinLocalState>();
	if (!right_projection_map.empty()) {
		state->build_chunk.Initialize(build_types);
	}
	for (auto &cond : conditions) {
		state->build_executor.AddExpression(*cond.right);
	}
	state->join_keys.Initialize(condition_types);
	return move(state);
}

void PhysicalHashJoin::Sink(ExecutionContext &context, GlobalOperatorState &state, LocalSinkState &lstate_p,
                            DataChunk &input) {

	auto &sink = (HashJoinGlobalState &)state;
	auto &lstate = (HashJoinLocalState &)lstate_p;
	// resolve the join keys for the right chunk
	lstate.build_executor.Execute(input, lstate.join_keys);
	// TODO: add statement to check for possible per
	// build the HT
	if (!right_projection_map.empty()) {
		// there is a projection map: fill the build chunk with the projected columns
		lstate.build_chunk.Reset();
		lstate.build_chunk.SetCardinality(input);
		for (idx_t i = 0; i < right_projection_map.size(); i++) {
			lstate.build_chunk.data[i].Reference(input.data[right_projection_map[i]]);
		}
		sink.hash_table->Build(lstate.join_keys, lstate.build_chunk);
	} else {
		// there is not a projected map: place the entire right chunk in the HT
		sink.hash_table->Build(lstate.join_keys, input);
	}
}

//===--------------------------------------------------------------------===//
// Finalize
//===--------------------------------------------------------------------===//
void PhysicalHashJoin::Finalize(Pipeline &pipeline, ClientContext &context, unique_ptr<GlobalOperatorState> state) {
	auto &sink = (HashJoinGlobalState &)*state;
	sink.hash_table->Finalize();
	// check for possible perfect hash table

	// We only do this optimization when all the requirements are true
	CheckRequirementsForPerfectHashJoin(sink.hash_table.get());

	PhysicalSink::Finalize(pipeline, context, move(state));
}

//===--------------------------------------------------------------------===//
// GetChunkInternal
//===--------------------------------------------------------------------===//
unique_ptr<PhysicalOperatorState> PhysicalHashJoin::GetOperatorState() {
	auto state = make_unique<PhysicalHashJoinState>(*this, children[0].get(), children[1].get(), conditions);
	state->cached_chunk.Initialize(types);
	state->join_keys.Initialize(condition_types);
	for (auto &cond : conditions) {
		state->probe_executor.AddExpression(*cond.left);
	}
	return move(state);
}

void PhysicalHashJoin::GetChunkInternal(ExecutionContext &context, DataChunk &chunk, PhysicalOperatorState *state_p) {
	auto state = reinterpret_cast<PhysicalHashJoinState *>(state_p);
	auto &sink = (HashJoinGlobalState &)*sink_state;
	if (sink.hash_table->size() == 0 &&
	    (sink.hash_table->join_type == JoinType::INNER || sink.hash_table->join_type == JoinType::SEMI)) {
		// empty hash table with INNER or SEMI join means empty result set
		return;
	}
	// We first try a probe with a perfect hash table, otherwise we do the normal probe
	if (IsInnerJoin(join_type) && ProbePerfectHashTable(context, chunk, state)) {
		return;
	}
	do {
		ProbeHashTable(context, chunk, state);
		if (chunk.size() == 0) {
#if STANDARD_VECTOR_SIZE >= 128
			if (state->cached_chunk.size() > 0) {
				// finished probing but cached data remains, return cached chunk
				chunk.Reference(state->cached_chunk);
				state->cached_chunk.Reset();
			} else
#endif
			    if (IsRightOuterJoin(join_type)) {
				// check if we need to scan any unmatched tuples from the RHS for the full/right outer join
				sink.hash_table->ScanFullOuter(chunk, sink.ht_scan_state);
			}
			return;
		} else {
#if STANDARD_VECTOR_SIZE >= 128
			if (chunk.size() < 64) {
				// small chunk: add it to chunk cache and continue
				state->cached_chunk.Append(chunk);
				if (state->cached_chunk.size() >= (STANDARD_VECTOR_SIZE - 64)) {
					// chunk cache full: return it
					chunk.Reference(state->cached_chunk);
					state->cached_chunk.Reset();
					return;
				} else {
					// chunk cache not full: probe again
					chunk.Reset();
				}
			} else {
				return;
			}
#else
			return;
#endif
		}
	} while (true);
}

void PhysicalHashJoin::ProbeHashTable(ExecutionContext &context, DataChunk &chunk, PhysicalOperatorState *state_p) {
	auto state = reinterpret_cast<PhysicalHashJoinState *>(state_p);
	auto &sink = (HashJoinGlobalState &)*sink_state;

	if (state->child_chunk.size() > 0 && state->scan_structure) {
		// still have elements remaining from the previous probe (i.e. we got
		// >1024 elements in the previous probe)
		state->scan_structure->Next(state->join_keys, state->child_chunk, chunk);
		if (chunk.size() > 0) {
			return;
		}
		state->scan_structure = nullptr;
	}

	// probe the HT
	do {
		// fetch the chunk from the left side
		children[0]->GetChunk(context, state->child_chunk, state->child_state.get());
		if (state->child_chunk.size() == 0) {
			return;
		}
		if (sink.hash_table->size() == 0) {
			ConstructEmptyJoinResult(sink.hash_table->join_type, sink.hash_table->has_null, state->child_chunk, chunk);
			return;
		}
		// resolve the join keys for the left chunk
		state->probe_executor.Execute(state->child_chunk, state->join_keys);

		// perform the actual probe
		state->scan_structure = sink.hash_table->Probe(state->join_keys);
		state->scan_structure->Next(state->join_keys, state->child_chunk, chunk);
	} while (chunk.size() == 0);
}

bool PhysicalHashJoin::ProbePerfectHashTable(ExecutionContext &context, DataChunk &result,
                                             PhysicalHashJoinState *physical_state) {

	// We only probe if the optimized hash table has been built
	if (!hasBuiltPerfectHashTable) {
		return false;
	}

	// fetch the chunk to join
	children[0]->GetChunk(context, physical_state->child_chunk, physical_state->child_state.get());
	if (physical_state->child_chunk.size() == 0) {
		// done with probing
		return true;
	}
	// fetch the  join keys
	physical_state->probe_executor.Execute(physical_state->child_chunk, physical_state->join_keys);

	// gets the data and selection vector
	VectorData keys_data;
	auto keys_size = physical_state->join_keys.size();  // number of keys
	auto keys_vec = &physical_state->join_keys.data[0]; // keys vector
	keys_vec->Orrify(keys_size, keys_data);             // get the keys data (Decompress) convert
	auto ldata = (int32_t *)keys_data.data; // cast to a typed buffer (TODO: switch case with multiple types)

	// go trough the join_keys and fill the selection vector with the matches
	SelectionVector matches(STANDARD_VECTOR_SIZE);
	size_t sel_idx {0};
	auto min_value = perfect_join_state.minimum.GetValue<int32_t>();
	auto max_value = perfect_join_state.maximum.GetValue<int32_t>();
	for (idx_t idx = 0; idx != physical_state->join_keys.size(); ++idx) {
		// a match means that ldata is in the range
		if (min_value <= ldata[idx] && ldata[idx] <= max_value) {
			matches.set_index(sel_idx++, idx);
		}
	}
	auto result_count = sel_idx;
	// slice for the left side
	result.Slice(physical_state->child_chunk, matches, result_count);

	// now get the data from the build side
	// first, set-up scan structure
	auto global_state = reinterpret_cast<HashJoinGlobalState *>(sink_state.get());
	auto hash_table_ptr = global_state->hash_table.get();
	auto scan_structure = make_unique<JoinHashTable::ScanStructure>(*hash_table_ptr);
	scan_structure->found_match = unique_ptr<bool[]>(new bool[STANDARD_VECTOR_SIZE]);
	memset(scan_structure->found_match.get(), 0, sizeof(bool) * STANDARD_VECTOR_SIZE);
	scan_structure->count = result_count;
	// hash all the keys
	Vector hashes(LogicalType::HASH);
	hash_table_ptr->Hash(physical_state->join_keys, matches, scan_structure->count, hashes);

	// now initialize the pointers of the scan structure based on the hashes
	hash_table_ptr->ApplyBitmask(hashes, matches, scan_structure->count, scan_structure->pointers);
	//	now gather the data from the build side
	idx_t offset = hash_table_ptr->condition_size;
	for (idx_t i = 0; i < hash_table_ptr->build_types.size(); i++) {
		auto &vector = result.data[physical_state->child_chunk.ColumnCount() + i];
		D_ASSERT(vector.GetType() == hash_table_ptr->build_types[i]);
		scan_structure->GatherResult(vector, matches, result_count, offset);
	}
	// scan_structure->AdvancePointers();

	return true;
}

void PhysicalHashJoin::CheckRequirementsForPerfectHashJoin(JoinHashTable *ht_ptr) {
	// first check the build size
	if (!perfect_join_state.is_build_small) {
		return;
	}
	// if built, just return to continue the probing

	// verify uniquiness (build size < min_max_range => duplicate values )
	auto build_size = Value::INTEGER(ht_ptr->size());
	auto min_max_range = perfect_join_state.maximum = perfect_join_state.minimum;
	if (build_size != min_max_range) {
		return;
	}
	// Store build side as a set of columns
	BuildPerfectHashStructure(ht_ptr);
	hasBuiltPerfectHashTable = true;

	// check for nulls
	if (ht_ptr->has_null) {
		// set selection vector
		return;
	}
}

void PhysicalHashJoin::BuildPerfectHashStructure(JoinHashTable *hash_table_ptr) {
	// allocate memory for each column in the hashtable
	std::vector<Vector> build_columns;
	auto build_size = hash_table_ptr->size();
	for (auto type : hash_table_ptr->build_types) {
		build_columns.emplace_back(type, build_size);
	}
}
} // namespace duckdb