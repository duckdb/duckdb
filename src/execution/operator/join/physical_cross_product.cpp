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
    return make_unique<CrossProductGlobalState>();
}

void PhysicalCrossProduct::Sink(ExecutionContext &context, GlobalOperatorState &state, LocalSinkState &lstate_,
                                DataChunk &input) {
    lock_guard<mutex> client_guard(rhs_lock);
	auto &sink = (CrossProductGlobalState &)state;
	sink.rhs_materialized.Append(input);
}

//===--------------------------------------------------------------------===//
// GetChunkInternal
//===--------------------------------------------------------------------===//
class PhysicalCrossProductOperatorState : public PhysicalOperatorState {
public:
	PhysicalCrossProductOperatorState(PhysicalOperator &op, PhysicalOperator *left, PhysicalOperator *right)
	    : PhysicalOperatorState(op, left), left_position(0), right_position(0) {
		D_ASSERT(left && right);
	}

	idx_t left_position;
	idx_t right_position;
};

unique_ptr<PhysicalOperatorState> PhysicalCrossProduct::GetOperatorState() {
	return make_unique<PhysicalCrossProductOperatorState>(*this, children[0].get(), children[1].get());
}

void PhysicalCrossProduct::GetChunkInternal(ExecutionContext &context, DataChunk &chunk,
                                            PhysicalOperatorState *state_) {
	auto state = reinterpret_cast<PhysicalCrossProductOperatorState *>(state_);
    auto &sink = (CrossProductGlobalState &)*sink_state;
	auto &right_collection = sink.rhs_materialized;

	if (sink.rhs_materialized.Count() == 0) {
		// no RHS: empty result
		return;
	}
	if (state->child_chunk.size() == 0 || state->right_position >= right_collection.Count()) {
		// ran out of entries on the RHS
		// reset the RHS and move to the next chunk on the LHS
		state->right_position = 0;
		children[0]->GetChunk(context, state->child_chunk, state->child_state.get());
		if (state->child_chunk.size() == 0) {
			// exhausted LHS: done
			return;
		}
	}

	auto &left_chunk = state->child_chunk;
	// now match the current vector of the left relation with the current row
	// from the right relation
	chunk.SetCardinality(left_chunk.size());
	for (idx_t i = 0; i < left_chunk.ColumnCount(); i++) {
		// first duplicate the values of the left side
		chunk.data[i].Reference(left_chunk.data[i]);
	}
	for (idx_t i = 0; i < right_collection.ColumnCount(); i++) {
		// now create a reference to the vectors of the right chunk
		auto rvalue = right_collection.GetValue(i, state->right_position);
		chunk.data[left_chunk.ColumnCount() + i].Reference(rvalue);
	}

	// for the next iteration, move to the next position on the right side
	state->right_position++;
}

} // namespace duckdb
