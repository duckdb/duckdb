#include "duckdb/execution/operator/set/physical_recursive_cte.hpp"

#include "duckdb/common/vector_operations/vector_operations.hpp"

#include "duckdb/common/types/chunk_collection.hpp"

using namespace duckdb;
using namespace std;

class PhysicalRecursiveCTEState : public PhysicalOperatorState {
public:
    PhysicalRecursiveCTEState() : PhysicalOperatorState(nullptr), top_done(false) {
    }
    unique_ptr<PhysicalOperatorState> top_state;
    unique_ptr<PhysicalOperatorState> bottom_state;
    bool top_done = false;

    bool recursing = false;
    bool intermediate_empty = true;
};

PhysicalRecursiveCTE::PhysicalRecursiveCTE(LogicalOperator &op, unique_ptr<PhysicalOperator> top, unique_ptr<PhysicalOperator> bottom)
        : PhysicalOperator(PhysicalOperatorType::RECURSIVE_CTE, op.types) {
    children.push_back(move(top));
    children.push_back(move(bottom));
}

// first exhaust top, then exhaust bottom. state to remember which.
void PhysicalRecursiveCTE::GetChunkInternal(ClientContext &context, DataChunk &chunk, PhysicalOperatorState *state_) {
    auto state = reinterpret_cast<PhysicalRecursiveCTEState *>(state_);

    if(!state->recursing) {
        do {
//            children[0]->GetChunk(context, state->child_chunk, state->child_state.get());
            children[0]->GetChunk(context, chunk, state->top_state.get());
            working_table->Append(chunk);
//            res.Append(state->child_chunk);
//            working_table.MaterializeWorkingtableChunk(chunk, state->working_position);
//            state->working_position += 1;
            if(chunk.size() != 0) return;
        } while (chunk.size() != 0);
        state->recursing = true;
    }

    while(true) {
        children[1]->GetChunk(context, chunk, state->bottom_state.get());

        if(chunk.size() == 0) {
            // Done if there is nothing in the intermediate table
            if (state->intermediate_empty) {
                state->finished = true;
                break;
            }

            working_table->count = 0;
            working_table->chunks.clear();

            working_table->count = intermediate_table.count;
            working_table->chunks = move(intermediate_table.chunks);

            intermediate_table.count = 0;
            intermediate_table.chunks.clear();

            (*iteration)++;

            state->bottom_state->finished = false;

            state->intermediate_empty = true;
            state->finished = false;
            continue;
        }

        state->intermediate_empty = false;
        intermediate_table.Append(chunk);
        return;
    }

}

unique_ptr<PhysicalOperatorState> PhysicalRecursiveCTE::GetOperatorState() {
    auto state = make_unique<PhysicalRecursiveCTEState>();
    state->top_state = children[0]->GetOperatorState();
    state->bottom_state = children[1]->GetOperatorState();
    return (move(state));
}

//string PhysicalRecursiveCTE::ExtraRenderInformation() const {
//    string extra_info;
//    for (auto &expr : this) {
//        extra_info += expr->GetName() + "\n";
//    }
//    return extra_info;
//}
