#include "duckdb/execution/operator/join/physical_cross_product.hpp"

#include "duckdb/common/vector_operations/vector_operations.hpp"

namespace duckdb {
using namespace std;

PhysicalCrossProduct::PhysicalCrossProduct(vector<LogicalType> types, unique_ptr<PhysicalOperator> left,
                                           unique_ptr<PhysicalOperator> right)
    : PhysicalSink(PhysicalOperatorType::CROSS_PRODUCT, move(types)) {
	children.push_back(move(left));
	children.push_back(move(right));
}

//===--------------------------------------------------------------------===//
// Sink
//===--------------------------------------------------------------------===//
class CrossProductGlobalState : public GlobalOperatorState {
public:
	CrossProductGlobalState() {
	}
	ChunkCollection rhs_materialized;
};

unique_ptr<GlobalOperatorState> PhysicalCrossProduct::GetGlobalState(ClientContext &context) {
    auto state = make_unique<CrossProductGlobalState>();
	return move(state);
}

void PhysicalCrossProduct::Sink(ExecutionContext &context, GlobalOperatorState &state, LocalSinkState &lstate_,
                                DataChunk &input) {
	auto &sink = (CrossProductGlobalState &)state;
	sink.rhs_materialized.Append(input);
}

//===--------------------------------------------------------------------===//
// GetChunkInternal
//===--------------------------------------------------------------------===//
class PhysicalCrossProductOperatorState : public PhysicalOperatorState {
public:
	PhysicalCrossProductOperatorState(PhysicalOperator &op, PhysicalOperator *left, PhysicalOperator *right)
	    : PhysicalOperatorState(op, left), left_position(0) {
		D_ASSERT(left && right);
	}

	idx_t left_position;
	idx_t right_position;
	ChunkCollection right_data;
};

unique_ptr<PhysicalOperatorState> PhysicalCrossProduct::GetOperatorState() {
	return make_unique<PhysicalCrossProductOperatorState>(*this, children[0].get(), children[1].get());
}

void PhysicalCrossProduct::GetChunkInternal(ExecutionContext &context, DataChunk &chunk,
                                            PhysicalOperatorState *state_) {
	auto state = reinterpret_cast<PhysicalCrossProductOperatorState *>(state_);
    auto &sink = (CrossProductGlobalState &)*sink_state;

    if (state->right_data.ColumnCount() == 0) {
		state->right_data.Append(sink.rhs_materialized);
		if (state->right_data.Count() == 0) {
			return;
		}
        state->left_position = 0;
		state->right_position = 0;
        children[0]->GetChunk(context, state->child_chunk, state->child_state.get());
        state->child_chunk.Normalify();
	}

	if (state->left_position >= state->child_chunk.size()) {
		return;
	}

	auto &left_chunk = state->child_chunk;
	auto &right_chunk = state->right_data.GetChunk(state->right_position);
	// now match the current row of the left relation with the current chunk
	// from the right relation
	chunk.SetCardinality(right_chunk.size());
	for (idx_t i = 0; i < left_chunk.ColumnCount(); i++) {
		// first duplicate the values of the left side
		auto lvalue = left_chunk.GetValue(i, state->left_position);
		chunk.data[i].Reference(lvalue);
	}
	for (idx_t i = 0; i < right_chunk.ColumnCount(); i++) {
		// now create a reference to the vectors of the right chunk
		chunk.data[left_chunk.ColumnCount() + i].Reference(right_chunk.data[i]);
	}

	// for the next iteration, move to the next position on the left side
	state->left_position++;
	if (state->left_position >= state->child_chunk.size()) {
		// ran out of this chunk
		// move to the next chunk on the right side
		state->left_position = 0;
		state->right_position++;
		if (state->right_position >= state->right_data.ChunkCount()) {
			state->right_position = 0;
			// move to the next chunk on the left side
			children[0]->GetChunk(context, state->child_chunk, state->child_state.get());
			state->child_chunk.Normalify();
		}
	}
}

} // namespace duckdb
