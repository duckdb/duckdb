#include "duckdb/execution/operator/join/physical_index_join.hpp"

#include "duckdb/common/vector_operations/vector_operations.hpp"
#include "duckdb/execution/expression_executor.hpp"
#include "duckdb/execution/index/art/art.hpp"
#include "duckdb/execution/operator/scan/physical_table_scan.hpp"
#include "duckdb/function/table/table_scan.hpp"
#include "duckdb/storage/buffer_manager.hpp"
#include "duckdb/storage/storage_manager.hpp"
#include "duckdb/transaction/transaction.hpp"

#include <iostream>
#include <utility>

using namespace std;

namespace duckdb {

class PhysicalIndexJoinOperatorState : public PhysicalOperatorState {
public:
	PhysicalIndexJoinOperatorState(PhysicalOperator &op, PhysicalOperator *left, PhysicalOperator *right)
	    : PhysicalOperatorState(op, left) {
		assert(left && right);
		for (idx_t i = 0; i < STANDARD_VECTOR_SIZE; i++) {
			rhs_rows.emplace_back();
			result_sizes.emplace_back();
		}
		output_sel.Initialize(STANDARD_VECTOR_SIZE);
	}

	idx_t lhs_idx = 0;
	idx_t rhs_idx = 0;
	idx_t result_size = 0;
	vector<idx_t> result_sizes;
	DataChunk join_keys;
	//! We store a vector of rhs data chunks, one for each key in the lhs_chunk
	//! Since one lhs_key might have more then one rhs_chunk as result we store it in a matrix
	vector<vector<row_t>> rhs_rows;
	ExpressionExecutor probe_executor;
	IndexLock lock;
	idx_t chunk_cur = 0;
	//! Stores the max matches found in this LHS chunk
	//! Used to decide how we will output data for this LHS chunk
	size_t max_matches = 0;
	//! Selection vector used in output per chunk
	SelectionVector output_sel;
	size_t output_sel_size = 0;
};

PhysicalIndexJoin::PhysicalIndexJoin(LogicalOperator &op, unique_ptr<PhysicalOperator> left,
                                     unique_ptr<PhysicalOperator> right, vector<JoinCondition> cond, JoinType join_type,
                                     const vector<idx_t> &left_projection_map, vector<idx_t> right_projection_map,
                                     vector<column_t> column_ids_, Index *index, bool lhs_first)
    : PhysicalOperator(PhysicalOperatorType::INDEX_JOIN, move(op.types)), left_projection_map(left_projection_map),
      right_projection_map(move(right_projection_map)), index(index), conditions(move(cond)), join_type(join_type),
      lhs_first(lhs_first) {
	column_ids = move(column_ids_);
	children.push_back(move(left));
	children.push_back(move(right));
	for (auto &condition : conditions) {
		condition_types.push_back(condition.left->return_type);
	}
	//! Only add to fetch_ids columns that are not indexed
	for (auto &index_id : index->column_ids) {
		index_ids.insert(index_id);
	}
	for (size_t column_id = 0; column_id < column_ids.size(); column_id++) {
		auto it = index_ids.find(column_ids[column_id]);
		if (it == index_ids.end()) {
			fetch_ids.push_back(column_ids[column_id]);
			fetch_types.push_back(children[1]->types[column_id]);
		}
	}
}

void PhysicalIndexJoin::OutputPerMatch(ExecutionContext &context, DataChunk &chunk, PhysicalOperatorState *state_) {
	auto state = reinterpret_cast<PhysicalIndexJoinOperatorState *>(state_);
	auto &transaction = Transaction::GetTransaction(context.client);
	auto &phy_tbl_scan = (PhysicalTableScan &)*children[1];
	auto &bind_tbl = (TableScanBindData &)*phy_tbl_scan.bind_data;
	auto tbl = bind_tbl.table->storage.get();
	DataChunk rhs_chunk;
	auto next_pos = state->result_sizes[state->lhs_idx] - state->rhs_idx > STANDARD_VECTOR_SIZE
	                    ? state->rhs_idx + STANDARD_VECTOR_SIZE
	                    : state->result_sizes[state->lhs_idx];
	if (!fetch_types.empty()) {
		rhs_chunk.Initialize(fetch_types);
		ColumnFetchState fetch_state;
		Vector row_ids;
		row_ids.type = LOGICAL_ROW_TYPE;
		FlatVector::SetData(row_ids, (data_ptr_t)&state->rhs_rows[state->lhs_idx][state->rhs_idx]);
		tbl->Fetch(transaction, rhs_chunk, fetch_ids, row_ids, next_pos - state->rhs_idx, fetch_state);
	}
	idx_t rhs_column_idx = 0;
	if (!lhs_first) {
		for (idx_t i = 0; i < right_projection_map.size(); i++) {
			auto it = index_ids.find(column_ids[right_projection_map[i]]);
			if (it == index_ids.end()) {
				chunk.data[i].Reference(rhs_chunk.data[rhs_column_idx++]);
			} else {
				auto rvalue = state->join_keys.GetValue(0, state->lhs_idx);
				chunk.data[i].Reference(rvalue);
			}
		}
		for (idx_t i = 0; i < left_projection_map.size(); i++) {
			auto lvalue = state->child_chunk.GetValue(left_projection_map[i], state->lhs_idx);
			chunk.data[right_projection_map.size() + i].Reference(lvalue);
		}
	} else {
		//! We have to duplicate LRS to number of matches
		for (idx_t i = 0; i < left_projection_map.size(); i++) {
			auto lvalue = state->child_chunk.GetValue(left_projection_map[i], state->lhs_idx);
			chunk.data[i].Reference(lvalue);
		}
		//! Add actual value
		//! We have to fetch RHS row based on the index ids
		for (idx_t i = 0; i < right_projection_map.size(); i++) {
			auto it = index_ids.find(column_ids[right_projection_map[i]]);
			if (it == index_ids.end()) {
				chunk.data[left_projection_map.size() + i].Reference(rhs_chunk.data[rhs_column_idx++]);
			} else {
				auto rvalue = state->join_keys.GetValue(0, state->lhs_idx);
				chunk.data[left_projection_map.size() + i].Reference(rvalue);
			}
		}
	}
	state->result_size = next_pos - state->rhs_idx;
	chunk.SetCardinality(state->result_size);
	state->rhs_idx = next_pos;
	if (state->rhs_idx == state->result_sizes[state->lhs_idx]) {
		//! We move to the next lhs value
		state->rhs_idx = 0;
		state->lhs_idx++;
	}
}

void PhysicalIndexJoin::OutputPerChunk(ExecutionContext &context, DataChunk &chunk, PhysicalOperatorState *state_) {
	auto &transaction = Transaction::GetTransaction(context.client);
	auto &phy_tbl_scan = (PhysicalTableScan &)*children[1];
	auto &bind_tbl = (TableScanBindData &)*phy_tbl_scan.bind_data;
	auto tbl = bind_tbl.table->storage.get();
	DataChunk rhs_chunk;
	idx_t rhs_column_idx = 0;
	SelectionVector sel;
	sel.Initialize(STANDARD_VECTOR_SIZE);
	size_t output_sel_idx{};
	auto state = reinterpret_cast<PhysicalIndexJoinOperatorState *>(state_);
	if (state->rhs_idx == state->max_matches) {
		//! We are done with this LHS Chunk
		state->lhs_idx = state->child_chunk.size();
		state->rhs_idx = 0;
		state->max_matches = 0;
		return;
	} else if (state->rhs_idx == 0) {
		//! First time accessing this LHS chunk
		for (idx_t i{}; i < state->result_sizes.size(); i++) {
			if (state->result_sizes[i] > 0) {
				sel.set_index(output_sel_idx++, i);
			}
		}
	} else {
		//! We need to update the selection vector to remove elements that don't have matches over rhs_idx
		for (idx_t i{}; i < state->output_sel_size; i++) {
			if (state->result_sizes[state->output_sel.get_index(i)] > state->rhs_idx) {
				sel.set_index(output_sel_idx++, state->result_sizes[state->output_sel.get_index(i)]);
			}
		}
	}
	//! Now we fetch the relevant data
	if (!fetch_types.empty()) {
		vector<row_t> fetch_rows;
		for (idx_t i{}; i < output_sel_idx; i++) {
			fetch_rows.push_back(state->rhs_rows[sel.get_index(i)][state->rhs_idx]);
		}
		rhs_chunk.Initialize(fetch_types);
		ColumnFetchState fetch_state;
		Vector row_ids;
		row_ids.type = LOGICAL_ROW_TYPE;
		FlatVector::SetData(row_ids, (data_ptr_t)&fetch_rows[0]);
		tbl->Fetch(transaction, rhs_chunk, fetch_ids, row_ids, output_sel_idx, fetch_state);
	}

	//! Now we actually output stuff
	if (!lhs_first) {
		for (idx_t i = 0; i < right_projection_map.size(); i++) {
			auto it = index_ids.find(column_ids[right_projection_map[i]]);
			if (it == index_ids.end()) {
				chunk.data[i].Reference(rhs_chunk.data[rhs_column_idx++]);
			} else {
				chunk.data[i].Reference(state->join_keys.data[0]);
				chunk.data[i].Slice(sel,output_sel_idx);
			}
		}
		for (idx_t i = 0; i < left_projection_map.size(); i++) {
			chunk.data[right_projection_map.size() + i].Reference(state->child_chunk.data[left_projection_map[i]]);
			chunk.data[right_projection_map.size() + i].Slice(sel,output_sel_idx);
		}
	} else {
		//! We have to duplicate LRS to number of matches
		for (idx_t i = 0; i < left_projection_map.size(); i++) {
			chunk.data[i].Reference(state->child_chunk.data[left_projection_map[i]]);
			chunk.data[i].Slice(sel,output_sel_idx);
		}
		//! Add actual value
		//! We have to fetch RHS row based on the index ids
		for (idx_t i = 0; i < right_projection_map.size(); i++) {
			auto it = index_ids.find(column_ids[right_projection_map[i]]);
			if (it == index_ids.end()) {
				chunk.data[left_projection_map.size() + i].Reference(rhs_chunk.data[rhs_column_idx++]);
			} else {
				chunk.data[left_projection_map.size() + i].Reference(state->join_keys.data[0]);
				chunk.data[left_projection_map.size() + i].Slice(sel,output_sel_idx);
			}
		}
	}
	//! Now we copy our selection vector to state and update the variables
	for (idx_t i{}; i < output_sel_idx; i++) {
		state->output_sel.set_index(i,sel.get_index(i));
	}
	state->result_size = output_sel_idx;
	chunk.SetCardinality(state->result_size);
	state->output_sel_size = output_sel_idx;
	state->rhs_idx++;
	state->lhs_idx++;
}

void PhysicalIndexJoin::GetRHSMatches(ExecutionContext &context, PhysicalOperatorState *state_) const {
	auto state = reinterpret_cast<PhysicalIndexJoinOperatorState *>(state_);
	auto &art = (ART &)*index;
	for (idx_t i = 0; i < state->child_chunk.size(); i++) {
		auto equal_value = state->join_keys.GetValue(0, i);
		state->rhs_rows[i].clear();
		if (!equal_value.is_null) {
			if (fetch_types.empty()) {
				//! Nothing to materialize
				art.SearchEqualJoinNoFetch(equal_value, state->result_sizes[i]);
				state->max_matches = max(state->max_matches, (size_t)state->result_sizes[i]);
			} else {
				art.SearchEqualJoin(equal_value, state->rhs_rows[i]);
				state->result_sizes[i] = state->rhs_rows[i].size();
				state->max_matches = max(state->max_matches, state->rhs_rows[i].size());
			}
		} else {
			//! This is null so no matches
			state->result_sizes[i] = 0;
		}
	}
	for (idx_t i = state->child_chunk.size(); i < STANDARD_VECTOR_SIZE; i++){
		//! No LHS chunk value so result size is empty
		state->result_sizes[i] = 0;
	}
}

void PhysicalIndexJoin::GetChunkInternal(ExecutionContext &context, DataChunk &chunk, PhysicalOperatorState *state_) {
	auto state = reinterpret_cast<PhysicalIndexJoinOperatorState *>(state_);
	if (!state->lock.index_lock) {
		index->InitializeLock(state->lock);
	}
	state->result_size = 0;
	while (state->result_size == 0) {
		//! Check if we need to get a new LHS chunk
		if (state->lhs_idx >= state->child_chunk.size()) {
			children[0]->GetChunk(context, state->child_chunk, state->child_state.get());
			if (state->child_chunk.size() == 0) {
				//! If chunk is empty there is nothing else to probe
				chunk.SetCardinality(state->result_size);
				return;
			}
			state->lhs_idx = 0;
			state->max_matches = 0;
			state->probe_executor.Execute(state->child_chunk, state->join_keys);
		}
		//! Fill Matches for the current LHS chunk
		if (state->lhs_idx == 0) {
			GetRHSMatches(context, state_);
		}
		//! Output vectors
		if (state->lhs_idx < state->child_chunk.size()) {
			if (state->max_matches > per_chunk_threshold) {
				//! At least one element is over 100 matches, lets output per match
				OutputPerMatch(context, chunk, state_);
			} else {
				OutputPerMatch(context, chunk, state_);
			}
		}
	}
}

unique_ptr<PhysicalOperatorState> PhysicalIndexJoin::GetOperatorState() {
	auto state = make_unique<PhysicalIndexJoinOperatorState>(*this, children[0].get(), children[1].get());
	if (right_projection_map.empty()) {
		for (column_t i = 0; i < column_ids.size(); i++) {
			right_projection_map.push_back(i);
		}
	}
	if (left_projection_map.empty()) {
		for (column_t i = 0; i < state->child_chunk.column_count(); i++) {
			left_projection_map.push_back(i);
		}
	}
	state->join_keys.Initialize(condition_types);
	for (auto &cond : conditions) {
		state->probe_executor.AddExpression(*cond.left);
	}
	return state;
}

} // namespace duckdb
