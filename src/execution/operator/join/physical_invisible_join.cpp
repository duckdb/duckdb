#include "duckdb/execution/operator/join/physical_invisible_join.hpp"

#include "duckdb/common/vector_operations/vector_operations.hpp"
#include "duckdb/execution/expression_executor.hpp"
#include "duckdb/execution/operator/join/physical_hash_join.hpp"
#include "duckdb/function/aggregate/distributive_functions.hpp"
#include "duckdb/storage/buffer_manager.hpp"
#include "duckdb/storage/storage_manager.hpp"

#include <iostream>
namespace duckdb {

PhysicalInvisibleJoin::PhysicalInvisibleJoin(LogicalOperator &op, unique_ptr<PhysicalOperator> left,
                                             unique_ptr<PhysicalOperator> right, vector<JoinCondition> cond,
                                             JoinType join_type, const vector<idx_t> &left_projection_map,
                                             const vector<idx_t> &right_projection_map_p,
                                             vector<LogicalType> delim_types, idx_t estimated_cardinality,
                                             PerfectHashJoinState join_state)
    : PhysicalComparisonJoin(op, PhysicalOperatorType::HASH_JOIN, move(cond), join_type, estimated_cardinality),
      right_projection_map(right_projection_map_p), delim_types(move(delim_types)), pjoin_state(join_state) {
	children.push_back(move(left));
	children.push_back(move(right));

	D_ASSERT(left_projection_map.size() == 0);
	for (auto &condition : conditions) {
		condition_types.push_back(condition.left->return_type);
	}
	build_types = LogicalOperator::MapTypes(children[1]->GetTypes(), right_projection_map);
}

//===--------------------------------------------------------------------===//
// Sink
//===--------------------------------------------------------------------===//

unique_ptr<GlobalOperatorState> PhysicalInvisibleJoin::GetGlobalState(ClientContext &context) {
	auto state = make_unique<HashJoinGlobalState>();
	state->hash_table =
	    make_unique<JoinHashTable>(BufferManager::GetBufferManager(context), conditions, build_types, join_type);
	// first set the build size
	const auto build_size = (pjoin_state.is_build_min_small) ? pjoin_state.estimated_cardinality + MIN_THRESHOLD
	                                                         : pjoin_state.estimated_cardinality;
	// init bool vector
	state->build_tuples = unique_ptr<bool[]>(new bool[build_size]);
	memset(state->build_tuples.get(), false, sizeof(bool) * build_size);
	return move(state);
}

void PhysicalInvisibleJoin::AppendToBuild(DataChunk &join_keys, DataChunk &build,
                                          std::vector<Vector> &build_columns) const {

	// for each column fill with build data
	for (idx_t i = 0; i != build_columns.size(); ++i) {
		// for each tuple
		for (idx_t i = 0; i != build.size(); ++i) {
			std::cout << "copy here" << std::endl;
		}
	}
}
unique_ptr<LocalSinkState> PhysicalInvisibleJoin::GetLocalSinkState(ExecutionContext &context) {
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

void PhysicalInvisibleJoin::Sink(ExecutionContext &context, GlobalOperatorState &state, LocalSinkState &lstate_p,
                                 DataChunk &input) const {
	auto &global_state = (HashJoinGlobalState &)state;
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
		global_state.hash_table->Build(lstate.join_keys, lstate.build_chunk);
	} else {
		// there is not a projected map: place the entire right chunk in the HT
		global_state.hash_table->Build(lstate.join_keys, input);
	}
	AppendToBuild(lstate.join_keys, input, global_state.build_columns);
}
//===--------------------------------------------------------------------===//
// Finalize
//===--------------------------------------------------------------------===//
bool PhysicalInvisibleJoin::Finalize(Pipeline &pipeline, ClientContext &context,
                                     unique_ptr<GlobalOperatorState> state) {
	auto &sink = (HashJoinGlobalState &)*state;
	// check for possible perfect hash table
	if (!CheckRequirementsForPerfectHashJoin(sink.hash_table.get(), sink)) {
		// no perfect hash table, just finish the building of the regular hash table
		sink.hash_table->Finalize();
	}

	PhysicalSink::Finalize(pipeline, context, move(state));
	return true;
}

//===--------------------------------------------------------------------===//
// GetChunkInternal
//===--------------------------------------------------------------------===//
unique_ptr<PhysicalOperatorState> PhysicalInvisibleJoin::GetOperatorState() {
	auto state = make_unique<PhysicalHashJoinState>(*this, children[0].get(), children[1].get(), conditions);
	state->cached_chunk.Initialize(types);
	state->join_keys.Initialize(condition_types);
	for (auto &cond : conditions) {
		state->probe_executor.AddExpression(*cond.left);
	}
	return move(state);
}

void PhysicalInvisibleJoin::GetChunkInternal(ExecutionContext &context, DataChunk &chunk,
                                             PhysicalOperatorState *state_p) {
	auto state = reinterpret_cast<PhysicalHashJoinState *>(state_p);
	auto &sink = (HashJoinGlobalState &)*sink_state;
	if (sink.hash_table->size() == 0 &&
	    (sink.hash_table->join_type == JoinType::INNER || sink.hash_table->join_type == JoinType::SEMI)) {
		// empty hash table with INNER or SEMI join means empty result set
		return;
	}
	// We first try a probe with an invisible join opt, otherwise we do the standard probe
	if (IsInnerJoin(join_type) && ExecuteInvisibleJoin(context, chunk, state, sink.hash_table.get())) {
		return;
	}
}

bool PhysicalInvisibleJoin::ExecuteInvisibleJoin(ExecutionContext &context, DataChunk &result,
                                                 PhysicalHashJoinState *physical_state, JoinHashTable *ht_ptr) {
	// We only probe if the optimized hash table has been built
	if (!hasInvisibleJoin) {
		return false;
	}

	// fetch the chunk to join
	children[0]->GetChunk(context, physical_state->child_chunk, physical_state->child_state.get());
	if (physical_state->child_chunk.size() == 0) {
		// no more keys to probe
		return true;
	}
	// fetch the join keys from the chunk
	physical_state->probe_executor.Execute(physical_state->child_chunk, physical_state->join_keys);
	// select the keys that are in the min-max range
	auto &keys_vec = physical_state->join_keys.data[0];
	Vector source(keys_vec.GetType());
	auto keys_count = physical_state->join_keys.size();
	SelectionVector sel_vec(keys_count);
	// FillSelectionVectorSwitch(keys_vec, sel_vec, keys_count);
	// copy the probe data to the result
	result.Reference(physical_state->child_chunk);
	// on the RHS, we need to fetch the data from the perfect hash table and slice it using the new selection vector
	for (idx_t i = 0; i < ht_ptr->build_types.size(); i++) {
		auto &res_vector = result.data[physical_state->child_chunk.ColumnCount() + i];
		D_ASSERT(res_vector.GetType() == ht_ptr->build_types[i]);
		auto &build_vec = ht_ptr->columns[i];
		res_vector.Reference(build_vec); //
		res_vector.Slice(sel_vec, keys_count);
	}
	return true;
}

bool PhysicalInvisibleJoin::CheckRequirementsForPerfectHashJoin(JoinHashTable *ht_ptr,
                                                                HashJoinGlobalState &join_state) {
	// first check the build size
	if (!pjoin_state.is_build_small) {
		return false;
	}

	// verify uniquiness (build size < min_max_range => duplicate values )
	if (ht_ptr->has_null) {
		return false;
	}

	// Store build side as a set of columns
	BuildPerfectHashStructure(ht_ptr, join_state.ht_scan_state, join_state.key_type);
	if (HasDuplicates(ht_ptr)) {
		return false;
	}
	hasInvisibleJoin = true;
	return true;
}

bool PhysicalInvisibleJoin::HasDuplicates(JoinHashTable *ht_ptr) {
	return false;
}

void PhysicalInvisibleJoin::BuildPerfectHashStructure(JoinHashTable *hash_table_ptr, JoinHTScanState &join_ht_state,
                                                      LogicalType key_type) {
	// allocate memory for each column
	auto build_size =
	    (pjoin_state.is_build_min_small) ? hash_table_ptr->size() + MIN_THRESHOLD : hash_table_ptr->size();
	for (auto type : hash_table_ptr->build_types) {
		hash_table_ptr->columns.emplace_back(type, build_size);
	}
	// Fill columns with build data
	hash_table_ptr->FullScanHashTable(join_ht_state, key_type);
}

template <typename T>
void PhysicalInvisibleJoin::TemplatedFillSelectionVector(Vector &source, SelectionVector &sel_vec, idx_t count) {
	auto min_value = pjoin_state.build_min.GetValue<T>();
	auto max_value = pjoin_state.build_max.GetValue<T>();
	pjoin_state.range = max_value - min_value;

	auto vector_data = FlatVector::GetData<T>(source);
	// generate the selection vector
	for (idx_t i = 0; i != count; ++i) {
		// add index to selection vector if value in the range
		auto input_value = vector_data[i];
		//		if (min_value <= input_value && input_value <= max_value) {
		auto idx = input_value;
		sel_vec.set_index(i, idx);
	}
	//	}
}

void PhysicalInvisibleJoin::FillSelectionVectorSwitch(Vector &source, SelectionVector &sel_vec, idx_t count) {
	switch (source.GetType().id()) {
	case LogicalTypeId::TINYINT:
		TemplatedFillSelectionVector<int8_t>(source, sel_vec, count);
		break;
	case LogicalTypeId::SMALLINT:
		TemplatedFillSelectionVector<int16_t>(source, sel_vec, count);
		break;
	case LogicalTypeId::INTEGER:
		TemplatedFillSelectionVector<int32_t>(source, sel_vec, count);
		break;
	case LogicalTypeId::BIGINT:
		TemplatedFillSelectionVector<int64_t>(source, sel_vec, count);
		break;
	case LogicalTypeId::UTINYINT:
		TemplatedFillSelectionVector<uint8_t>(source, sel_vec, count);
		break;
	case LogicalTypeId::USMALLINT:
		TemplatedFillSelectionVector<uint16_t>(source, sel_vec, count);
		break;
	case LogicalTypeId::UINTEGER:
		TemplatedFillSelectionVector<uint32_t>(source, sel_vec, count);
		break;
	case LogicalTypeId::UBIGINT:
		TemplatedFillSelectionVector<uint64_t>(source, sel_vec, count);
		break;
	default:
		throw NotImplementedException("Type not supported");
		break;
	}
}

} // namespace duckdb
