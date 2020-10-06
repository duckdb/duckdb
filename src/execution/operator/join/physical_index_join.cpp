#include "duckdb/common/vector_operations/vector_operations.hpp"
#include "duckdb/execution/expression_executor.hpp"
#include "duckdb/execution/operator/join/physical_index_join.hpp"

#include <utility>
#include "duckdb/function/aggregate/distributive_functions.hpp"
#include "duckdb/storage/buffer_manager.hpp"
#include "duckdb/storage/storage_manager.hpp"

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
            // Probe the index
		    // Perform tuple reconstruction for RHS projection
        } while (chunk.size() == 0);

	    int a = 0;
	    return;
    }
    void PhysicalIndexJoin::Sink(ExecutionContext &context, GlobalOperatorState &state, LocalSinkState &lstate, DataChunk &input){
	    return;
    }
	void PhysicalIndexJoin::Finalize(ClientContext &context, unique_ptr<GlobalOperatorState> state){
	    return;
    }
} // namespace duckdb