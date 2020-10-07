#include "duckdb/common/vector_operations/vector_operations.hpp"
#include "duckdb/execution/expression_executor.hpp"
#include "duckdb/execution/operator/join/physical_index_join.hpp"

#include <utility>
#include "duckdb/function/aggregate/distributive_functions.hpp"
#include "duckdb/storage/buffer_manager.hpp"
#include "duckdb/storage/storage_manager.hpp"
#include "duckdb/transaction/transaction.hpp"
#include "duckdb/execution/index/art/art.hpp"
#include "duckdb/execution/operator/scan/physical_table_scan.hpp"
#include "duckdb/function/table/table_scan.hpp"
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
	    DataChunk* lhs_data = nullptr;
    };

    PhysicalIndexJoin::PhysicalIndexJoin(LogicalOperator &op, unique_ptr<PhysicalOperator> left, unique_ptr<PhysicalOperator> right,
	                  vector<JoinCondition> cond, JoinType join_type, const vector<idx_t>& left_projection_map,
	                  vector<idx_t> right_projection_map, Index *index)
        : PhysicalOperator(PhysicalOperatorType::INDEX_JOIN, move(op.types)),
          right_projection_map(std::move(right_projection_map)), index (index),conditions(move(cond)), join_type(join_type) {
        children.push_back(move(left));
        children.push_back(move(right));
        assert(left_projection_map.empty());
        for (auto &condition : conditions) {
            condition_types.push_back(condition.left->return_type);
        }
    }

    void PhysicalIndexJoin::GetChunkInternal(ExecutionContext &context, DataChunk &chunk, PhysicalOperatorState *state_){
	    auto state = reinterpret_cast<PhysicalIndexJoinOperatorState *>(state_);
	    size_t cur_vec_size = 0;
	    auto &art = (ART &)*index;
	    auto &transaction = Transaction::GetTransaction(context.client);
	    auto &phy_tbl_scan = (PhysicalTableScan &)*children[1];
	    auto &bind_tbl = (TableScanBindData &)*phy_tbl_scan.bind_data;
	    auto tbl = bind_tbl.table->storage.get();
	    if (!state->lhs_data){
		    if (right_projection_map.empty()){
			    // this means we should project all columns
			    for (size_t i = 0; i < children[1]->types.size()-1;i++){
				    right_projection_map.push_back(i);
			    }
			    right_projection_map.push_back(COLUMN_IDENTIFIER_ROW_ID);
	        }
		    children[0]->GetChunk(context, state->child_chunk, state->child_state.get());
		    state->lhs_data = &state->child_chunk;
	    }
	    while (state->left_position <state->lhs_data->size()) {
		    //! Probe the index
		    for (;state->left_position < state->child_chunk.size(); state->left_position++) {
			    vector<row_t> result_ids;
			    auto equal_value = state->child_chunk.data.data()->GetValue(state->left_position);
			    unique_ptr<IndexScanState> t_state =
			        index->InitializeScanSinglePredicate(transaction, equal_value, ExpressionType::COMPARE_EQUAL);
			    auto i_state = (ARTIndexScanState *)t_state.get();
			    art.Scan(transaction,*tbl,*i_state,STANDARD_VECTOR_SIZE,result_ids);
			    DataChunk rhs_tuple;
			    rhs_tuple.Initialize(children[1]->types);
			    ColumnFetchState fetch_state;
			    Vector row_ids;
			    row_ids.type = LOGICAL_ROW_TYPE;
			    FlatVector::SetData(row_ids, (data_ptr_t)&result_ids[0]);
			    tbl->Fetch(transaction, rhs_tuple, right_projection_map, row_ids,
			               result_ids.size(), fetch_state);
			    for (;state->last_match < result_ids.size(); state->last_match++) {
				    //! We have to duplicate LRS to number of matches
				    for (idx_t i = 0; i < state->child_chunk.column_count(); i++) {
					    auto lvalue = state->child_chunk.GetValue(i, state->left_position);
					    chunk.data[i].Reference(lvalue);
				    }
				    //! Add actual value
				    //! We have to fetch RHS row based on the index ids
				    for (idx_t i = 0; i < right_projection_map.size(); i++) {
					    auto rvalue = rhs_tuple.GetValue(i, state->last_match);
		                chunk.data[state->child_chunk.column_count() + i].Reference(rvalue);
				    }
				    cur_vec_size++;
				    if (cur_vec_size == STANDARD_VECTOR_SIZE){
					    chunk.SetCardinality(cur_vec_size);
					    return;
				    }
			    }
			    state->last_match = 0;
		    }
		    children[0]->GetChunk(context, state->child_chunk, state->child_state.get());
		    state->left_position = 0;
		    if (state->child_chunk.size() == 0) {
			    chunk.SetCardinality(cur_vec_size);
			    return;
		    }
	    }
    }

    unique_ptr<PhysicalOperatorState> PhysicalIndexJoin::GetOperatorState() {
	    return make_unique<PhysicalIndexJoinOperatorState>(*this, children[0].get(), children[1].get());
    }

} // namespace duckdb