
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

	// first we fully materialize the right child, if we haven't done that yet
	if (state->right_chunks.size() == 0) {
		auto right_state = children[1]->GetOperatorState(state->parent);
		do {
			auto new_chunk = make_unique<DataChunk>();
			children[1]->InitializeChunk(*new_chunk.get());
			children[1]->GetChunk(*new_chunk.get(), right_state.get());

			if (new_chunk->count == 0) {
				break;
			}
			state->right_chunks.push_back(move(new_chunk));
		} while (true);

		if (state->right_chunks.size() == 0) {
			return;
		}
	}
	// now that we have fully materialized the right child
	// we have to perform the cross product
	// for every row in the left, we have to create a copy of every row in the
	// right

	// first check if we have to fetch a new chunk from the left child
	if (state->left_position >= state->child_chunk.count) {
		// if we have exhausted the current left chunk, fetch a new one
		children[0]->GetChunk(state->child_chunk, state->child_state.get());
		if (state->child_chunk.count == 0) {
			return;
		}
		state->left_position = 0;
		state->right_chunk = 0;
	}
	auto &left_chunk = state->child_chunk;
	auto &right_chunk = *state->right_chunks[state->right_chunk].get();
	assert(right_chunk.count <= chunk.maximum_size);
	// now match the current row of the left relation with the current chunk
	// from the right relation
	chunk.count = right_chunk.count;
	for (size_t i = 0; i < left_chunk.column_count; i++) {
		// first duplicate the values of the left side using a selection vector
		chunk.data[i].count = chunk.count;
		VectorOperations::Set(
		    chunk.data[i], left_chunk.data[i].GetValue(state->left_position));
	}
	for (size_t i = 0; i < right_chunk.column_count; i++) {
		// now create a reference to the vectors of the right chunk
		chunk.data[left_chunk.column_count + i].Reference(right_chunk.data[i]);
	}

	// for the next iteration, move to the next chunk
	state->right_chunk++;
	if (state->right_chunk >= state->right_chunks.size()) {
		// if we have exhausted all the chunks, move to the next tuple in the
		// left set
		state->left_position++;
		state->right_chunk = 0;
	}
}

std::unique_ptr<PhysicalOperatorState>
PhysicalCrossProduct::GetOperatorState(ExpressionExecutor *parent_executor) {
	return make_unique<PhysicalCrossProductOperatorState>(
	    children[0].get(), children[1].get(), parent_executor);
}
