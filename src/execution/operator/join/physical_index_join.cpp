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

#include <utility>
using namespace std;

namespace duckdb {
class PhysicalIndexJoinOperatorState : public PhysicalOperatorState {
public:
	PhysicalIndexJoinOperatorState(PhysicalOperator &op, PhysicalOperator *left, PhysicalOperator *right)
	    : PhysicalOperatorState(op, left), left_position(0) {
		assert(left && right);
	}

	idx_t left_position = 0;
	idx_t last_match = 0;
	DataChunk join_keys;
	ExpressionExecutor probe_executor;
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

void PhysicalIndexJoin::GetChunkInternal(ExecutionContext &context, DataChunk &chunk, PhysicalOperatorState *state_) {
	auto state = reinterpret_cast<PhysicalIndexJoinOperatorState *>(state_);
	size_t cur_vec_size = 0;
	auto &art = (ART &)*index;
	auto &transaction = Transaction::GetTransaction(context.client);
	auto &phy_tbl_scan = (PhysicalTableScan &)*children[1];
	auto &bind_tbl = (TableScanBindData &)*phy_tbl_scan.bind_data;
	auto tbl = bind_tbl.table->storage.get();

	while (state->left_position <= state->child_chunk.size()) {
		if (state->child_chunk.size() == state->left_position){
			//! Get a new chunk
            children[0]->GetChunk(context, state->child_chunk, state->child_state.get());
			if (state->child_chunk.size() == 0) {
				//! If chunk is empty there is nothing else to probe
			    chunk.SetCardinality(cur_vec_size);
			    return;
		    }
			state->last_match = 0;
			state->left_position = 0;
			state->probe_executor.Execute(state->child_chunk, state->join_keys);
		}
		//! Probe the index
		for (; state->left_position < state->child_chunk.size(); state->left_position++) {
			vector<row_t> result_ids;
			auto equal_value = state->join_keys.GetValue(0,state->left_position);
			unique_ptr<IndexScanState> t_state =
			    index->InitializeScanSinglePredicate(transaction, equal_value, ExpressionType::COMPARE_EQUAL);
			auto i_state = (ARTIndexScanState *)t_state.get();
			art.Scan(transaction, *tbl, *i_state, STANDARD_VECTOR_SIZE, result_ids);
			DataChunk rhs_tuple;
			rhs_tuple.Initialize(children[1]->types);
			ColumnFetchState fetch_state;
			Vector row_ids;
			row_ids.type = LOGICAL_ROW_TYPE;
			FlatVector::SetData(row_ids, (data_ptr_t)&result_ids[0]);
			tbl->Fetch(transaction, rhs_tuple, column_ids, row_ids, result_ids.size(), fetch_state);
			for (; state->last_match < result_ids.size(); state->last_match++) {
				if (!lhs_first) {
					for (idx_t i = 0; i < right_projection_map.size(); i++) {
						auto rvalue = rhs_tuple.GetValue(right_projection_map[i], state->last_match);
						chunk.data[i].SetValue(cur_vec_size, rvalue);
					}
					for (idx_t i = 0; i < left_projection_map.size(); i++) {
						auto lvalue = state->child_chunk.GetValue(left_projection_map[i], state->left_position);
						chunk.data[right_projection_map.size() + i].SetValue(cur_vec_size, lvalue);
					}
				} else {
					//! We have to duplicate LRS to number of matches
					for (idx_t i = 0; i < left_projection_map.size(); i++) {
						auto lvalue = state->child_chunk.GetValue(left_projection_map[i], state->left_position);
						chunk.data[i].SetValue(cur_vec_size, lvalue);
					}
					//! Add actual value
					//! We have to fetch RHS row based on the index ids
					for (idx_t i = 0; i < right_projection_map.size(); i++) {
						auto rvalue = rhs_tuple.GetValue(right_projection_map[i], state->last_match);
						chunk.data[state->child_chunk.column_count() + i].SetValue(cur_vec_size, rvalue);
					}
				}
				cur_vec_size++;
				if (cur_vec_size == STANDARD_VECTOR_SIZE) {
					state->last_match++;
					chunk.SetCardinality(STANDARD_VECTOR_SIZE);
					return;
				}
			}
			state->last_match = 0;
		}
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
