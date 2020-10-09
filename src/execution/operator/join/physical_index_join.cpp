#include "duckdb/execution/operator/join/physical_index_join.hpp"

#include "duckdb/common/vector_operations/vector_operations.hpp"
#include "duckdb/execution/expression_executor.hpp"
#include "duckdb/execution/index/art/art.hpp"
#include "duckdb/execution/operator/scan/physical_table_scan.hpp"
#include "duckdb/function/aggregate/distributive_functions.hpp"
#include "duckdb/function/table/table_scan.hpp"
#include "duckdb/storage/buffer_manager.hpp"
#include "duckdb/storage/storage_manager.hpp"
#include "duckdb/transaction/transaction.hpp"
#include "iostream"
#include <utility>
using namespace std;

namespace duckdb {

PhysicalIndexJoin::PhysicalIndexJoin(LogicalOperator &op, unique_ptr<PhysicalOperator> left,
                                     unique_ptr<PhysicalOperator> right, vector<JoinCondition> cond, JoinType join_type,
                                     const vector<idx_t> &left_projection_map, vector<idx_t> right_projection_map,
                                     vector<column_t> column_ids, Index *index, bool lhs_first)
    : PhysicalOperator(PhysicalOperatorType::INDEX_JOIN, move(op.types)), column_ids(move(column_ids)),
      left_projection_map(left_projection_map), right_projection_map(move(right_projection_map)), index(index),
      conditions(move(cond)), join_type(join_type), lhs_first(lhs_first) {
	children.push_back(move(left));
	children.push_back(move(right));
	for (auto &condition : conditions) {
		condition_types.push_back(condition.left->return_type);
	}
}

bool PhysicalIndexJoin::FillResultChunk(PhysicalIndexJoinOperatorState *state, DataChunk &chunk, idx_t &result_size) {
	for (; state->rhs_idx < state->rhs_chunk.size(); state->rhs_idx++) {
		if (!lhs_first) {
			for (idx_t i = 0; i < right_projection_map.size(); i++) {
				auto rvalue = state->rhs_chunk.GetValue(right_projection_map[i], state->rhs_idx);
				chunk.data[i].SetValue(result_size, rvalue);
			}
			for (idx_t i = 0; i < left_projection_map.size(); i++) {
				auto lvalue = state->child_chunk.GetValue(left_projection_map[i], state->lhs_idx);
				chunk.data[right_projection_map.size() + i].SetValue(result_size, lvalue);
			}
		} else {
			//! We have to duplicate LRS to number of matches
			for (idx_t i = 0; i < left_projection_map.size(); i++) {
				auto lvalue = state->child_chunk.GetValue(left_projection_map[i], state->lhs_idx);
				chunk.data[i].SetValue(result_size, lvalue);
			}
			//! Add actual value
			//! We have to fetch RHS row based on the index ids
			for (idx_t i = 0; i < right_projection_map.size(); i++) {
				auto rvalue = state->rhs_chunk.GetValue(right_projection_map[i], state->rhs_idx);
				chunk.data[state->child_chunk.column_count() + i].SetValue(result_size, rvalue);
			}
		}
		result_size++;
		if (result_size == STANDARD_VECTOR_SIZE) {
			state->rhs_idx++;
			chunk.SetCardinality(STANDARD_VECTOR_SIZE);
			return true;
		}
	}
	return false;
}

void PhysicalIndexJoin::GetRHSChunk(ExecutionContext &context, PhysicalIndexJoinOperatorState *state) {
	auto &art = (ART &)*index;
	auto &transaction = Transaction::GetTransaction(context.client);
	auto &phy_tbl_scan = (PhysicalTableScan &)*children[1];
	auto &bind_tbl = (TableScanBindData &)*phy_tbl_scan.bind_data;
	auto tbl = bind_tbl.table->storage.get();
	auto idx_state_ = (ARTIndexScanState *)state->idx_state.get();
	vector<row_t> result_ids;
	if (art.SearchEqualJoin(idx_state_, result_ids)) {
		state->idx_state.reset();
		assert(!state->idx_state);
	}
	state->rhs_chunk.Initialize(children[1]->types);
	ColumnFetchState fetch_state;
	Vector row_ids;
	row_ids.type = LOGICAL_ROW_TYPE;
	FlatVector::SetData(row_ids, (data_ptr_t)&result_ids[0]);
	tbl->Fetch(transaction, state->rhs_chunk, column_ids, row_ids, result_ids.size(), fetch_state);
	state->rhs_idx = 0;
}

void PhysicalIndexJoin::SetProbe(ExecutionContext &context, PhysicalIndexJoinOperatorState *state) {
	auto &transaction = Transaction::GetTransaction(context.client);
	auto equal_value = state->join_keys.GetValue(0, state->lhs_idx);
	state->idx_state = index->InitializeScanSinglePredicate(transaction, equal_value, ExpressionType::COMPARE_EQUAL);
}

void PhysicalIndexJoin::GetChunkInternal(ExecutionContext &context, DataChunk &chunk, PhysicalOperatorState *state_) {
	auto state = reinterpret_cast<PhysicalIndexJoinOperatorState *>(state_);
	size_t result_size = 0;
	while (result_size < STANDARD_VECTOR_SIZE) {
		//! Check if we need to get a new LHS chunk
		if (state->get_new_chunk) {
			cerr<< state->cur_chunk << endl;
			children[0]->GetChunk(context, state->child_chunk, state->child_state.get());
			if (state->child_chunk.size() == 0) {
				//! If chunk is empty there is nothing else to probe
				chunk.SetCardinality(result_size);
				return;
			}
			state->lhs_idx = 0;
			state->rhs_idx = 0;
			state->get_new_chunk = false;
			state->probe_executor.Execute(state->child_chunk, state->join_keys);
			state->cur_chunk++;
		}
		//! Iterate over LHS chunk
		for (; state->lhs_idx < state->child_chunk.size(); state->lhs_idx++) {
			//! First, Check if we must continue from a previous unfinished RHS Chunk
			if (FillResultChunk(state, chunk, result_size)) {
				return;
			} else {
				assert(state->rhs_idx == state->rhs_chunk.size());
				//! Our result chunk is not full, this means we need to fetch a new RHS chunk
				//! Check if we are still scanning a leaf
				if (state->idx_state) {
					//! If still scanning leaf, fetch RHS chunk and Fill Result
					GetRHSChunk(context, state);
					if (FillResultChunk(state, chunk, result_size)) {
						return;
					}
				} else {
					//! If we are not Scanning the leaf we move to the next LHS key
					SetProbe(context, state);
					//! We get a chunk and fill result
					GetRHSChunk(context, state);
					if (FillResultChunk(state, chunk, result_size)) {
						state->lhs_idx++;
						return;
					}
				}
			}
		}
		//! We finished Scanning this chunk
		state->get_new_chunk = true;
	}
}

unique_ptr<PhysicalOperatorState> PhysicalIndexJoin::GetOperatorState() {
	auto state = make_unique<PhysicalIndexJoinOperatorState>(*this, children[0].get(), children[1].get());
	if (right_projection_map.empty()) {
		for (size_t i = 0; i < column_ids.size(); i++) {
			right_projection_map.push_back(i);
		}
	}
	if (left_projection_map.empty()) {
		for (size_t i = 0; i < state->child_chunk.column_count(); i++) {
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
