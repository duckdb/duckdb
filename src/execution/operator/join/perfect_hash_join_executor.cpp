#include "duckdb/execution/operator/join/perfect_hash_join_executor.hpp"

#include "duckdb/common/vector_operations/vector_operations.hpp"
#include "duckdb/execution/expression_executor.hpp"
#include "duckdb/execution/operator/join/physical_hash_join.hpp"
#include "duckdb/function/aggregate/distributive_functions.hpp"
#include "duckdb/storage/buffer_manager.hpp"
#include "duckdb/storage/storage_manager.hpp"

#include <iostream>
namespace duckdb {

bool PerfectHashJoinExecutor::ProbePerfectHashTable(ExecutionContext &context, DataChunk &result,
                                                    PhysicalHashJoinState *physical_state, JoinHashTable *ht_ptr,
                                                    PhysicalOperator *operator_child) {
	// We only probe if the optimized hash table has been built

	// fetch the chunk to join
	operator_child->GetChunk(context, physical_state->child_chunk, physical_state->child_state.get());
	if (physical_state->child_chunk.size() == 0) {
		// no more keys to probe
		return true;
	}
	// fetch the join keys from the chunk
	physical_state->probe_executor.Execute(physical_state->child_chunk, physical_state->join_keys);
	// select the keys that are in the min-max range
	auto &keys_vec = physical_state->join_keys.data[0];
	auto keys_count = physical_state->join_keys.size();
	SelectionVector sel_vec(keys_count);
	FillSelectionVectorSwitch(keys_vec, sel_vec, keys_count);
	// reference the probe data to the result
	result.Reference(physical_state->child_chunk);
	// on the RHS, we need to fetch the data from the build structure and slice it using the new selection vector
	for (idx_t i = 0; i < ht_ptr->build_types.size(); i++) {
		auto &res_vector = result.data[physical_state->child_chunk.ColumnCount() + i];
		D_ASSERT(res_vector.GetType() == ht_ptr->build_types[i]);
		auto &build_vec = ht_ptr->columnar_hash_table[i];
		res_vector.Reference(build_vec); //
		res_vector.Slice(sel_vec, keys_count);
	}
	return true;
}

bool PerfectHashJoinExecutor::CheckForPerfectHashJoin(JoinHashTable *ht_ptr, PerfectHashJoinStats &pjoin_state) {
	// first check the build size
	if (!pjoin_state.is_build_small) {
		return false;
	}

	// check for nulls
	if (ht_ptr->has_null) {
		return false;
	}
	return true;
}

void PerfectHashJoinExecutor::BuildPerfectHashTable(JoinHashTable *hash_table_ptr, JoinHTScanState &join_ht_state,
                                                    LogicalType key_type, PerfectHashJoinStats &pjoin_state) {
	// allocate memory for each column
	auto build_size =
	    (pjoin_state.is_build_min_small) ? hash_table_ptr->size() + MIN_THRESHOLD : hash_table_ptr->size();
	for (auto type : hash_table_ptr->build_types) {
		hash_table_ptr->columnar_hash_table.emplace_back(type, build_size);
	}
	// Fill columns with build data
	hash_table_ptr->FullScanHashTable(join_ht_state, key_type);
}

template <typename T>
void PerfectHashJoinExecutor::TemplatedFillSelectionVector(Vector &source, SelectionVector &sel_vec, idx_t count) {
	/* 	auto min_value = pjoin_state.build_min.GetValue<T>();
	    auto max_value = pjoin_state.build_max.GetValue<T>(); */

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

void PerfectHashJoinExecutor::FillSelectionVectorSwitch(Vector &source, SelectionVector &sel_vec, idx_t count) {
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

/* void PerfectHashJoinExecutor::ComputeIndex() {
    idx_t current_shift = total_required_bits;
    for (idx_t i = 0; i < groups.ColumnCount(); i++) {
        current_shift -= required_bits[i];
        ComputeGroupLocation(groups.data[i], group_minima[i], address_data, current_shift, groups.size());
    }
} */
} // namespace duckdb
