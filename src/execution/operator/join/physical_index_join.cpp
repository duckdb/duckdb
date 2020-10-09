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

class PhysicalIndexJoinOperatorState : public PhysicalOperatorState {
public:
	PhysicalIndexJoinOperatorState(PhysicalOperator &op, PhysicalOperator *left, PhysicalOperator *right)
	    : PhysicalOperatorState(op, left) {
		assert(left && right);
	}

	idx_t lhs_idx = 0;
	idx_t rhs_idx = 0;
	DataChunk join_keys;
	DataChunk rhs_chunk;
	ExpressionExecutor probe_executor;
	bool get_new_chunk = true;
	unique_ptr<IndexScanState> idx_state;
	idx_t cur_chunk = 0;
	bool first_row = true;
	IndexLock lock;

	void GetRHSChunk(ExecutionContext &context, Index &index, PhysicalOperator &rhs, vector<column_t> &column_ids);
	//! Fills result chunk.
	//! Returns True is result chunk is already full. False OW
	inline bool FillResultChunk(DataChunk &chunk, idx_t &result_size, bool lhs_first,
	                            vector<idx_t> &left_projection_map, vector<idx_t> &right_projection_map);
	//! Set Element to probe the index with
	inline bool SetProbe(ExecutionContext &context, Index &index);
};

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

bool PhysicalIndexJoinOperatorState::FillResultChunk(DataChunk &chunk, idx_t &result_size, bool lhs_first,
                                                     vector<idx_t> &left_projection_map,
                                                     vector<idx_t> &right_projection_map) {
	for (; rhs_idx < rhs_chunk.size(); rhs_idx++) {
		if (!lhs_first) {
			for (idx_t i = 0; i < right_projection_map.size(); i++) {
				auto rvalue = rhs_chunk.GetValue(right_projection_map[i], rhs_idx);
				chunk.data[i].SetValue(result_size, rvalue);
			}
			for (idx_t i = 0; i < left_projection_map.size(); i++) {
				auto lvalue = child_chunk.GetValue(left_projection_map[i], lhs_idx);
				chunk.data[right_projection_map.size() + i].SetValue(result_size, lvalue);
			}
		} else {
			//! We have to duplicate LRS to number of matches
			for (idx_t i = 0; i < left_projection_map.size(); i++) {
				auto lvalue = child_chunk.GetValue(left_projection_map[i], lhs_idx);
				chunk.data[i].SetValue(result_size, lvalue);
			}
			//! Add actual value
			//! We have to fetch RHS row based on the index ids
			for (idx_t i = 0; i < right_projection_map.size(); i++) {
				auto rvalue = rhs_chunk.GetValue(right_projection_map[i], rhs_idx);
				chunk.data[child_chunk.column_count() + i].SetValue(result_size, rvalue);
			}
		}
		result_size++;
		if (result_size == STANDARD_VECTOR_SIZE) {
			rhs_idx++;
			chunk.SetCardinality(STANDARD_VECTOR_SIZE);
			return true;
		}
	}
	return false;
}

void PhysicalIndexJoinOperatorState::GetRHSChunk(ExecutionContext &context, Index &index, PhysicalOperator &rhs,
                                                 vector<column_t> &column_ids) {
	auto &art = (ART &)index;
	auto &transaction = Transaction::GetTransaction(context.client);
	auto &phy_tbl_scan = (PhysicalTableScan &)rhs;
	auto &bind_tbl = (TableScanBindData &)*phy_tbl_scan.bind_data;
	auto tbl = bind_tbl.table->storage.get();
	auto idx_state_ = (ARTIndexScanState *)idx_state.get();
	vector<row_t> result_ids;
	if (art.SearchEqualJoin(idx_state_, result_ids)) {
		idx_state.reset();
		assert(!idx_state);
	}
	rhs_chunk.Initialize(rhs.types);
	ColumnFetchState fetch_state;
	Vector row_ids;
	row_ids.type = LOGICAL_ROW_TYPE;
	FlatVector::SetData(row_ids, (data_ptr_t)&result_ids[0]);
	tbl->Fetch(transaction, rhs_chunk, column_ids, row_ids, result_ids.size(), fetch_state);
	rhs_idx = 0;
}

bool PhysicalIndexJoinOperatorState::SetProbe(ExecutionContext &context, Index &index) {
	auto &transaction = Transaction::GetTransaction(context.client);
	auto equal_value = join_keys.GetValue(0, lhs_idx);
	if (!equal_value.is_null) {
		idx_state = index.InitializeScanSinglePredicate(transaction, equal_value, ExpressionType::COMPARE_EQUAL);
		return true;
	}
	return false;
}

void PhysicalIndexJoin::GetChunkInternal(ExecutionContext &context, DataChunk &chunk, PhysicalOperatorState *state_) {
	auto state = reinterpret_cast<PhysicalIndexJoinOperatorState *>(state_);
	if (!state->lock.index_lock) {
		index->InitializeLock(state->lock);
	}
	idx_t result_size = 0;
	while (result_size < STANDARD_VECTOR_SIZE) {
		//! Check if we need to get a new LHS chunk
		if (state->get_new_chunk) {
			children[0]->GetChunk(context, state->child_chunk, state->child_state.get());
			if (state->child_chunk.size() == 0) {
				//! If chunk is empty there is nothing else to probe
				chunk.SetCardinality(result_size);
				return;
			}
			state->first_row = true;
			state->lhs_idx = 0;
			state->rhs_idx = 0;
			state->rhs_chunk.Reset();
			state->get_new_chunk = false;
			state->probe_executor.Execute(state->child_chunk, state->join_keys);
			state->cur_chunk++;
		}
		//! Iterate over LHS chunk
		while (state->lhs_idx < state->child_chunk.size()) {
			//! First, Check if we must continue from a previous unfinished RHS Chunk
			if (state->FillResultChunk(chunk, result_size, lhs_first, left_projection_map, right_projection_map)) {
				return;
			} else {
				assert(state->rhs_idx == state->rhs_chunk.size());
				//! Our result chunk is not full, this means we need to fetch a new RHS chunk
				//! Check if we are still scanning a leaf
				if (state->idx_state) {
					//! If still scanning leaf, fetch RHS chunk and Fill Result
					state->GetRHSChunk(context, *index, *(children[1]), column_ids);
					if (state->FillResultChunk(chunk, result_size, lhs_first, left_projection_map,
					                           right_projection_map)) {
						return;
					}
				} else {
					if (state->first_row) {
						state->first_row = false;
					} else {
						state->lhs_idx++;
					}
					if (state->lhs_idx == state->child_chunk.size()) {
						break;
					}
					//! If we are not Scanning the leaf we move to the next LHS key
					if (state->SetProbe(context, *index)) {
						//! We get a chunk and fill result
						state->GetRHSChunk(context, *index, *(children[1]), column_ids);
						if (state->FillResultChunk(chunk, result_size, lhs_first, left_projection_map,
						                           right_projection_map)) {
							return;
						}
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
