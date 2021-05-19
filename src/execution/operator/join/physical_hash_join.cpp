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
                            DataChunk &input) const {
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
bool PhysicalHashJoin::Finalize(Pipeline &pipeline, ClientContext &context, unique_ptr<GlobalOperatorState> state) {
	auto &sink = (HashJoinGlobalState &)*state;
	// We only do this optimization when all the requirements are true
	// check for possible perfect hash table
	CheckRequirementsForPerfectHashJoin(sink.hash_table.get(), sink);
	if (!hasBuiltPerfectHashTable) {
		sink.hash_table->Finalize();
	}

	PhysicalSink::Finalize(pipeline, context, move(state));
	return true;
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
	// We first try a probe with a perfect hash table, otherwise we do the standard probe
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
	// select the keys that are in the min-max range
	Vector matches;
	auto &keys_vec = physical_state->join_keys.data[0];
	auto keys_count = physical_state->join_keys.size();
	MinMaxRangeSwitch(keys_vec, matches, keys_count);
	// slice for the left side
	result.Slice(physical_state->child_chunk, FlatVector::INCREMENTAL_SELECTION_VECTOR, keys_count);
	result.Print();

	// now get the data from the build side
	// first, set-up scan structure

	return true;
}

void PhysicalHashJoin::CheckRequirementsForPerfectHashJoin(JoinHashTable *ht_ptr, HashJoinGlobalState &join_state) {
	// first check the build size
	if (!perfect_join_state.is_build_small) {
		return;
	}

	// verify uniquiness (build size < min_max_range => duplicate values )
	auto build_size = Value::INTEGER(ht_ptr->size());
	auto build_range = perfect_join_state.build_max - perfect_join_state.build_min;
	if (build_size != build_range + 1) {
		return;
	}
	// Store build side as a set of columns
	BuildPerfectHashStructure(ht_ptr, join_state.ht_scan_state);
	hasBuiltPerfectHashTable = true;

	// check for nulls
	if (ht_ptr->has_null) {
		// set selection vector
		return;
	}
}

void PhysicalHashJoin::BuildPerfectHashStructure(JoinHashTable *hash_table_ptr, JoinHTScanState &join_ht_state) {
	// allocate memory for each column in the hashtable
	auto build_size = hash_table_ptr->size();
	for (auto type : hash_table_ptr->build_types) {
		hash_table_ptr->columns.emplace_back(type, build_size);
	}
	// Fill columns with build data
	hash_table_ptr->FullScanHashTable(join_ht_state);
}

template <typename T>
void PhysicalHashJoin::TemplatedMinMaxRange(Vector &source, Vector &result, idx_t count) {
	auto min_value = perfect_join_state.build_min.GetValue<T>();
	auto max_value = perfect_join_state.build_max.GetValue<T>();

	UnaryExecutor::Execute<T, T>(source, result, count, [&](T input_value) {
		// a match means that input is in the range
		if (min_value <= input_value && input_value <= max_value) {
			return input_value;
		}
		return NullValue<T>();
	});
}

void PhysicalHashJoin::MinMaxRangeSwitch(Vector &source, Vector &result, idx_t count) {
	// now switch on the source type
	switch (source.GetType().id()) {
	case LogicalTypeId::BOOLEAN:
		TemplatedMinMaxRange<bool>(source, result, count);
		break;
	case LogicalTypeId::TINYINT:
		TemplatedMinMaxRange<int8_t>(source, result, count);
		break;
	case LogicalTypeId::SMALLINT:
		TemplatedMinMaxRange<int16_t>(source, result, count);
		break;
	case LogicalTypeId::INTEGER:
		TemplatedMinMaxRange<int32_t>(source, result, count);
		break;
	case LogicalTypeId::BIGINT:
		TemplatedMinMaxRange<int64_t>(source, result, count);
		break;
	case LogicalTypeId::UTINYINT:
		TemplatedMinMaxRange<uint8_t>(source, result, count);
		break;
	case LogicalTypeId::USMALLINT:
		TemplatedMinMaxRange<uint16_t>(source, result, count);
		break;
	case LogicalTypeId::UINTEGER:
		TemplatedMinMaxRange<uint32_t>(source, result, count);
		break;
	case LogicalTypeId::UBIGINT:
		TemplatedMinMaxRange<uint64_t>(source, result, count);
		break;
	case LogicalTypeId::HUGEINT:
		TemplatedMinMaxRange<hugeint_t>(source, result, count);
		break;
	case LogicalTypeId::FLOAT:
		TemplatedMinMaxRange<float>(source, result, count);
		break;
	case LogicalTypeId::DOUBLE:
		TemplatedMinMaxRange<double>(source, result, count);
		break;
	default:
		throw NotImplementedException("Type not supported");
		break;
	}
}
} // namespace duckdb
