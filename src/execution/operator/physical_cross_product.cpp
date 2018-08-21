
#include "execution/operator/physical_cross_product.hpp"
#include "common/types/vector_operations.hpp"

using namespace duckdb;
using namespace std;

PhysicalCrossProduct::PhysicalCrossProduct(
    std::unique_ptr<PhysicalOperator> left,
    std::unique_ptr<PhysicalOperator> right)
    : PhysicalOperator(PhysicalOperatorType::CROSS_PRODUCT) {
	children.push_back(move(left));
	children.push_back(move(right));
}

vector<TypeId> PhysicalCrossProduct::GetTypes() {
	auto left = children[0]->GetTypes();
	auto right = children[1]->GetTypes();
	left.insert(left.end(), right.begin(), right.end());
	return left;
}

void PhysicalCrossProduct::GetChunk(DataChunk &chunk,
                                    PhysicalOperatorState *state_) {
	auto state = reinterpret_cast<PhysicalCrossProductOperatorState *>(state_);
	chunk.Reset();

	if (!state->right_state) {
		// no right state: initialize right and left chunks
		// left chunk
		children[0]->GetChunk(state->child_chunk, state->child_state.get());
		if (state->child_chunk.count == 0) {
			return;
		}
		state->left_position = 0;
		// right chunk: start over from beginning
		children[1]->InitializeChunk(state->right_chunk);
		state->right_state = children[1]->GetOperatorState(state->parent);
		children[1]->GetChunk(state->right_chunk, state->right_state.get());
	}

	auto &left_chunk = state->child_chunk;
	auto &right_chunk = state->right_chunk;
	if (right_chunk.sel_vector) {
		right_chunk.Flatten();
	}
	// now match the current row of the left relation with the current chunk
	// from the right relation
	chunk.count = right_chunk.count;
	for (size_t i = 0; i < left_chunk.column_count; i++) {
		// first duplicate the values of the left side
		chunk.data[i].count = chunk.count;
		VectorOperations::Set(
		    chunk.data[i], left_chunk.data[i].GetValue(state->left_position));
	}
	for (size_t i = 0; i < right_chunk.column_count; i++) {
		// now create a reference to the vectors of the right chunk
		chunk.data[left_chunk.column_count + i].Reference(right_chunk.data[i]);
	}

	// for the next iteration, move to the next position on the left side
	state->left_position++;
	if (state->left_position >= state->child_chunk.count) {
		// ran out of this chunk
		// move to the next chunk on the right side
		state->left_position = 0;
		children[1]->GetChunk(state->right_chunk, state->right_state.get());
		if (state->right_chunk.count == 0) {
			// ran out of chunks on the right side
			// move to the next left chunk and start over on the right hand side
			state->right_state = nullptr;
		}
	}

	chunk.Verify();
}

std::unique_ptr<PhysicalOperatorState>
PhysicalCrossProduct::GetOperatorState(ExpressionExecutor *parent_executor) {
	return make_unique<PhysicalCrossProductOperatorState>(
	    children[0].get(), children[1].get(), parent_executor);
}
