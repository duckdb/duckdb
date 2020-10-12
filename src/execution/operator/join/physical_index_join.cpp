#include "duckdb/execution/operator/join/physical_index_join.hpp"

#include "duckdb/common/vector_operations/vector_operations.hpp"
#include "duckdb/execution/expression_executor.hpp"
#include "duckdb/execution/index/art/art.hpp"
#include "duckdb/execution/operator/scan/physical_table_scan.hpp"
#include "duckdb/function/table/table_scan.hpp"
#include "duckdb/storage/buffer_manager.hpp"
#include "duckdb/storage/storage_manager.hpp"
#include "duckdb/transaction/transaction.hpp"

#include <utility>

#include <iostream>

using namespace std;

namespace duckdb {

class PhysicalIndexJoinOperatorState : public PhysicalOperatorState {
public:
	PhysicalIndexJoinOperatorState(PhysicalOperator &op, PhysicalOperator *left, PhysicalOperator *right)
	    : PhysicalOperatorState(op, left) {
		assert(left && right);
		for (idx_t i = 0; i < STANDARD_VECTOR_SIZE; i++) {
			rhs_chunks.emplace_back();
			result_sizes.emplace_back();
		}
	}

	idx_t lhs_idx = 0;
	idx_t rhs_idx = 0;
	idx_t result_size = 0;
	vector<vector<idx_t>> result_sizes;
	DataChunk join_keys;
	//! We store a vector of rhs data chunks, one for each key in the lhs_chunk
	//! Since one lhs_key might have more then one rhs_chunk as result we store it in a matrix
	vector<vector<unique_ptr<DataChunk>>> rhs_chunks;
	ExpressionExecutor probe_executor;
	unique_ptr<IndexScanState> idx_state;
	IndexLock lock;
	idx_t chunk_cur = 0;

	void GetRHSChunk(ExecutionContext &context, Index &index, PhysicalOperator &rhs, vector<column_t> &fetch_ids,
	                 vector<LogicalType> &fetch_types, bool reset);
	//! Fills the result chunk and outputs one vector match per execution. Used when one LHS key has tons of matches
	inline bool OutputPerMatch(DataChunk &chunk, bool lhs_first, vector<column_t> &left_projection_map,
	                                    vector<column_t> &right_projection_map, vector<column_t> &column_ids,
	                                    unordered_set<column_t> index_ids);
	//! Set Element to probe the index with
	inline bool SetProbe(ExecutionContext &context, Index &index, idx_t lhs_idx_);

	//! Fills the result chunk and outputs depending on the element with the highest number of matches of the LHS
	//! Chunk
	bool FillResultChunkPerChunk(DataChunk &chunk, bool lhs_first, vector<column_t> &left_projection_map,
	                             vector<column_t> &right_projection_map, vector<column_t> &column_ids,
	                             unordered_set<column_t> index_ids);
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

bool PhysicalIndexJoinOperatorState::OutputPerMatch(DataChunk &chunk, bool lhs_first,
                                                             vector<column_t> &left_projection_map,
                                                             vector<column_t> &right_projection_map,
                                                             vector<column_t> &column_ids,
                                                             unordered_set<column_t> index_ids) {
	result_size = result_sizes[lhs_idx][rhs_idx];
	chunk.SetCardinality(result_size);
	if (result_size == 0) {
		//! Nothing to output
		lhs_idx++;
		rhs_idx = 0;
		return false;
	}
	idx_t rhs_column_idx = 0;
	if (!lhs_first) {
		for (idx_t i = 0; i < right_projection_map.size(); i++) {
			auto it = index_ids.find(column_ids[right_projection_map[i]]);
			if (it == index_ids.end()) {
				chunk.data[i].Reference(rhs_chunks[lhs_idx][rhs_idx]->data[rhs_column_idx++]);
			} else {
				auto rvalue = join_keys.GetValue(0, lhs_idx);
				chunk.data[i].Reference(rvalue);
			}
		}
		for (idx_t i = 0; i < left_projection_map.size(); i++) {
			auto lvalue = child_chunk.GetValue(left_projection_map[i], lhs_idx);
			chunk.data[right_projection_map.size() + i].Reference(lvalue);
		}
	} else {
		//! We have to duplicate LRS to number of matches
		for (idx_t i = 0; i < left_projection_map.size(); i++) {
			auto lvalue = child_chunk.GetValue(left_projection_map[i], lhs_idx);
			chunk.data[i].Reference(lvalue);
		}
		//! Add actual value
		//! We have to fetch RHS row based on the index ids
		for (idx_t i = 0; i < right_projection_map.size(); i++) {
			auto it = index_ids.find(column_ids[right_projection_map[i]]);
			if (it == index_ids.end()) {
				chunk.data[left_projection_map.size() + i].Reference(
				    rhs_chunks[lhs_idx][rhs_idx]->data[rhs_column_idx++]);
			} else {
				auto rvalue = join_keys.GetValue(0, lhs_idx);
				chunk.data[left_projection_map.size() + i].Reference(rvalue);
			}
		}
	}
	rhs_idx++;
	if (rhs_idx == rhs_chunks[lhs_idx].size()) {
		//! We are done with this rhs vector chain, move to the next rhs chunk chain
		rhs_idx = 0;
		lhs_idx++;
	}

	return false;
}

bool PhysicalIndexJoinOperatorState::FillResultChunkPerChunk(DataChunk &chunk, bool lhs_first,
                                                             vector<column_t> &left_projection_map,
                                                             vector<column_t> &right_projection_map,
                                                             vector<column_t> &column_ids,
                                                             unordered_set<column_t> index_ids) {

	assert(0);
	chunk.SetCardinality(result_size);
	return false;
}

void PhysicalIndexJoinOperatorState::GetRHSChunk(ExecutionContext &context, Index &index, PhysicalOperator &rhs,
                                                 vector<column_t> &fetch_ids, vector<LogicalType> &fetch_types,
                                                 bool reset) {
	auto &art = (ART &)index;
	auto &transaction = Transaction::GetTransaction(context.client);
	auto &phy_tbl_scan = (PhysicalTableScan &)rhs;
	auto &bind_tbl = (TableScanBindData &)*phy_tbl_scan.bind_data;
	auto tbl = bind_tbl.table->storage.get();
	auto idx_state_ = (ARTIndexScanState *)idx_state.get();
	vector<row_t> result_ids;
	unique_ptr<DataChunk> rhs_chunk = make_unique<DataChunk>();
	if (reset) {
		rhs_chunks[lhs_idx].clear();
		result_sizes[lhs_idx].clear();
	}

	if (art.SearchEqualJoin(idx_state_, result_ids)) {
		idx_state.reset();
		assert(!idx_state);
	}
	result_sizes[lhs_idx].push_back(result_ids.size());
	if (fetch_types.empty()) {
		//! Nothing to materialize
		rhs_chunks[lhs_idx].push_back(move(rhs_chunk));
		return;
	}
	rhs_chunk->Initialize(fetch_types);
	ColumnFetchState fetch_state;
	Vector row_ids;
	row_ids.type = LOGICAL_ROW_TYPE;
	FlatVector::SetData(row_ids, (data_ptr_t)&result_ids[0]);
	tbl->Fetch(transaction, *rhs_chunk, fetch_ids, row_ids, result_ids.size(), fetch_state);
	rhs_chunks[lhs_idx].push_back(move(rhs_chunk));
}

bool PhysicalIndexJoinOperatorState::SetProbe(ExecutionContext &context, Index &index, idx_t lhs_idx_) {
	auto &transaction = Transaction::GetTransaction(context.client);
	auto equal_value = join_keys.GetValue(0, lhs_idx_);
	if (equal_value.is_null) {
		return false;

	} else {
		idx_state = index.InitializeScanSinglePredicate(transaction, equal_value, ExpressionType::COMPARE_EQUAL);
		return true;
	}
}
void PhysicalIndexJoin::GetAllRHSChunks(ExecutionContext &context, PhysicalOperatorState *state_) {
	auto state = reinterpret_cast<PhysicalIndexJoinOperatorState *>(state_);
	for (idx_t i = 0; i < state->child_chunk.size(); i++) {
		state->SetProbe(context, *index, i);
		if (!state->idx_state) {
			state->rhs_chunks[state->lhs_idx].clear();
			state->result_sizes[state->lhs_idx].clear();
			state->result_sizes[state->lhs_idx].push_back({0});
		}
		else{
			//! Get first RHS chunk
			state->GetRHSChunk(context, *index, *(children[1]), fetch_ids, fetch_types, true);
		}
		while (state->idx_state) {
			//! If we have more than one RHS chunk for that key, add it in the chain
			state->GetRHSChunk(context, *index, *(children[1]), fetch_ids, fetch_types, false);
		}
		state->lhs_idx++;
	}
}

void PhysicalIndexJoin::GetChunkInternal(ExecutionContext &context, DataChunk &chunk, PhysicalOperatorState *state_) {
	auto state = reinterpret_cast<PhysicalIndexJoinOperatorState *>(state_);
	if (!state->lock.index_lock) {
		index->InitializeLock(state->lock);
	}
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
			state->probe_executor.Execute(state->child_chunk, state->join_keys);
		}
		//! Iterate over LHS chunk
		//! Fill rhs_chunks with the matches for each LHS chunk key
		if (state->lhs_idx == 0) {
			GetAllRHSChunks(context, state_);
			state->lhs_idx = 0;
		}
		//! Output vectors
		if (state->lhs_idx < state->child_chunk.size()) {
			state->OutputPerMatch(chunk, lhs_first, left_projection_map, right_projection_map, column_ids, index_ids);
		}
	}
	state->result_size = 0;
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
