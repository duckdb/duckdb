#include "duckdb/common/vector_operations/vector_operations.hpp"
#include "duckdb/execution/expression_executor.hpp"
#include "duckdb/execution/operator/join/physical_index_join.hpp"

#include <utility>
#include "duckdb/function/aggregate/distributive_functions.hpp"
#include "duckdb/storage/buffer_manager.hpp"
#include "duckdb/storage/storage_manager.hpp"
#include "duckdb/transaction/transaction.hpp"
#include "duckdb/execution/index/art/art.hpp"
using namespace std;

namespace duckdb {
    PhysicalIndexJoin::PhysicalIndexJoin(LogicalOperator &op, unique_ptr<PhysicalOperator> left, unique_ptr<PhysicalOperator> right,
	                  vector<JoinCondition> cond, JoinType join_type, const vector<idx_t>& left_projection_map,
	                  vector<idx_t> right_projection_map, Index *index)
        : PhysicalComparisonJoin(op, PhysicalOperatorType::HASH_JOIN, move(cond), join_type), index (index),
          right_projection_map(std::move(right_projection_map)) {
        children.push_back(move(left));
        children.push_back(move(right));
        assert(left_projection_map.empty());
        for (auto &condition : conditions) {
            condition_types.push_back(condition.left->return_type);
        }
    }

    void PhysicalIndexJoin::GetChunkInternal(ExecutionContext &context, DataChunk &chunk, PhysicalOperatorState *state){
	    // probe the index
        do {
            // fetch the chunk from the left side
            children[0]->GetChunk(context, state->child_chunk, state->child_state.get());
            if (state->child_chunk.size() == 0) {
                return;
            }
		    // resolve the join keys for the left chunk
            // Probe the index
		    auto& art = (ART&)*index;
		    SelectionVector result_vector(STANDARD_VECTOR_SIZE);
		    for (size_t i = 0; i < state->child_chunk.size(); i ++){
	            vector<row_t> result_ids;
                auto equal_value = state->child_chunk.data.data()->GetValue(i);
		        auto &transaction = Transaction::GetTransaction(context.client);
		        unique_ptr<IndexScanState> t_state = index->InitializeScanSinglePredicate(transaction, equal_value, ExpressionType::COMPARE_EQUAL);
			    auto i_state = (ARTIndexScanState *)t_state.get();
			    art.SearchEqual(i_state,STANDARD_VECTOR_SIZE,result_ids);
			    for (size_t j = 0; j  < result_ids.size(); j ++){
				    auto &vector = chunk.data[0];
				    auto &vector_2 = chunk.data[1];
				    vector.SetValue(chunk.size(),equal_value);
				    vector_2.SetValue(chunk.size(),equal_value);
				    chunk.SetCardinality(chunk.size()+1);
			    }
		    }
        } while (chunk.size() == 0);

	    return;
    }
    void PhysicalIndexJoin::Sink(ExecutionContext &context, GlobalOperatorState &state, LocalSinkState &lstate, DataChunk &input){
	    return;
    }
	void PhysicalIndexJoin::Finalize(ClientContext &context, unique_ptr<GlobalOperatorState> state){
	    return;
    }
} // namespace duckdb