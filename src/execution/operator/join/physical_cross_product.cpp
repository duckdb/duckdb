#include "duckdb/execution/operator/join/physical_cross_product.hpp"

#include "duckdb/common/vector_operations/vector_operations.hpp"

using namespace duckdb;
using namespace std;

class PhysicalCrossProductOperatorState : public PhysicalOperatorState {
public:
	PhysicalCrossProductOperatorState(PhysicalOperator *left, PhysicalOperator *right)
	    : PhysicalOperatorState(left), left_position(0) {
		assert(left && right);
	}

	idx_t left_position;
	idx_t right_position;
	ChunkCollection right_data;
};

PhysicalCrossProduct::PhysicalCrossProduct(LogicalOperator &op, unique_ptr<PhysicalOperator> left,
                                           unique_ptr<PhysicalOperator> right)
    : PhysicalOperator(PhysicalOperatorType::CROSS_PRODUCT, op.types) {
	children.push_back(move(left));
	children.push_back(move(right));
}

void PhysicalCrossProduct::GetChunkInternal(ClientContext &context, DataChunk &chunk, PhysicalOperatorState *state_) {
	auto state = reinterpret_cast<PhysicalCrossProductOperatorState *>(state_);
	// first we fully materialize the right child, if we haven't done that yet
	if (state->right_data.column_count() == 0) {
		auto right_state = children[1]->GetOperatorState();
		auto types = children[1]->GetTypes();

		DataChunk new_chunk;
		new_chunk.Initialize(types);
		do {
			children[1]->GetChunk(context, new_chunk, right_state.get());
			if (new_chunk.size() == 0) {
				break;
			}
			state->right_data.Append(new_chunk);
		} while (new_chunk.size() > 0);

		if (state->right_data.count == 0) {
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
	auto &right_chunk = *state->right_data.chunks[state->right_position];
	// now match the current row of the left relation with the current chunk
	// from the right relation
	chunk.SetCardinality(right_chunk.size());
	for (idx_t i = 0; i < left_chunk.column_count(); i++) {
		// first duplicate the values of the left side
		auto lvalue = left_chunk.GetValue(i, state->left_position);
		chunk.data[i].Reference(lvalue);
	}
	for (idx_t i = 0; i < right_chunk.column_count(); i++) {
		// now create a reference to the vectors of the right chunk
		chunk.data[left_chunk.column_count() + i].Reference(right_chunk.data[i]);
	}

	// for the next iteration, move to the next position on the left side
	state->left_position++;
	if (state->left_position >= state->child_chunk.size()) {
		// ran out of this chunk
		// move to the next chunk on the right side
		state->left_position = 0;
		state->right_position++;
		if (state->right_position >= state->right_data.chunks.size()) {
			state->right_position = 0;
			// move to the next chunk on the left side
			children[0]->GetChunk(context, state->child_chunk, state->child_state.get());
			state->child_chunk.Normalify();
		}
	}
}

unique_ptr<PhysicalOperatorState> PhysicalCrossProduct::GetOperatorState() {
	return make_unique<PhysicalCrossProductOperatorState>(children[0].get(), children[1].get());
}
