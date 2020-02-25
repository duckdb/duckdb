#include "duckdb/execution/operator/order/physical_top_n.hpp"

#include "duckdb/common/assert.hpp"
#include "duckdb/common/value_operations/value_operations.hpp"
#include "duckdb/common/vector_operations/vector_operations.hpp"
#include "duckdb/execution/expression_executor.hpp"
#include "duckdb/storage/data_table.hpp"

using namespace duckdb;
using namespace std;

class PhysicalTopNOperatorState : public PhysicalOperatorState {
public:
	PhysicalTopNOperatorState(PhysicalOperator *child) : PhysicalOperatorState(child), position(0) {
	}

	idx_t position;
	idx_t current_offset;
	ChunkCollection sorted_data;
	unique_ptr<idx_t[]> heap;
};

void PhysicalTopN::GetChunkInternal(ClientContext &context, DataChunk &chunk, PhysicalOperatorState *state_) {
	auto state = reinterpret_cast<PhysicalTopNOperatorState *>(state_);
	ChunkCollection &big_data = state->sorted_data;

	if (state->position == 0) {
		// first concatenate all the data of the child chunks
		do {
			children[0]->GetChunk(context, state->child_chunk, state->child_state.get());
			big_data.Append(state->child_chunk);
		} while (state->child_chunk.size() != 0);

		// now perform the actual ordering of the data
		// compute the sorting columns from the input data
		ExpressionExecutor executor;
		vector<TypeId> sort_types;
		vector<OrderType> order_types;
		for (idx_t i = 0; i < orders.size(); i++) {
			auto &expr = orders[i].expression;
			sort_types.push_back(expr->return_type);
			order_types.push_back(orders[i].type);
			executor.AddExpression(*expr);
		}

		CalculateHeapSize(big_data.count);
		if (heap_size == 0) {
			return;
		}

		ChunkCollection heap_collection;
		for (idx_t i = 0; i < big_data.chunks.size(); i++) {
			DataChunk heap_chunk;
			heap_chunk.Initialize(sort_types);

			executor.Execute(*big_data.chunks[i], heap_chunk);
			heap_collection.Append(heap_chunk);
		}

		assert(heap_collection.count == big_data.count);

		// create and use the heap
		state->heap = unique_ptr<idx_t[]>(new idx_t[heap_size]);
		heap_collection.Heap(order_types, state->heap.get(), heap_size);
	}

	if (state->position >= heap_size) {
		return;
	} else if (state->position < offset) {
		state->position = offset;
	}

	state->position += big_data.MaterializeHeapChunk(chunk, state->heap.get(), state->position, heap_size);
}

unique_ptr<PhysicalOperatorState> PhysicalTopN::GetOperatorState() {
	return make_unique<PhysicalTopNOperatorState>(children[0].get());
}

void PhysicalTopN::CalculateHeapSize(idx_t rows) {
	heap_size = (rows > offset) ? min(limit + offset, rows) : 0;
}
