#include "execution/operator/order/physical_order.hpp"

#include "common/assert.hpp"
#include "common/value_operations/value_operations.hpp"
#include "common/vector_operations/vector_operations.hpp"
#include "execution/expression_executor.hpp"
#include "storage/data_table.hpp"

using namespace duckdb;
using namespace std;


void PhysicalOrder::_GetChunk(ClientContext &context, DataChunk &chunk, PhysicalOperatorState *state_) {
	auto state = reinterpret_cast<PhysicalOrderOperatorState *>(state_);
	ChunkCollection &big_data = state->sorted_data;
	if (state->position == 0) {
		// first concatenate all the data of the child chunks
		do {
			children[0]->GetChunk(context, state->child_chunk, state->child_state.get());
			big_data.Append(state->child_chunk);
		} while (state->child_chunk.size() != 0);

		// now perform the actual ordering of the data
		// compute the sorting columns from the input data
		vector<TypeId> sort_types;
		for (size_t i = 0; i < description.orders.size(); i++) {
			auto &expr = description.orders[i].expression;
			sort_types.push_back(expr->return_type);
		}

		ChunkCollection sort_collection;
		for (size_t i = 0; i < big_data.chunks.size(); i++) {
			DataChunk sort_chunk;
			sort_chunk.Initialize(sort_types);

			ExpressionExecutor executor(*big_data.chunks[i], context);
			executor.Execute(sort_chunk, [&](size_t i) { return description.orders[i].expression.get(); },
			                 description.orders.size());
			sort_collection.Append(sort_chunk);
		}

		if (sort_collection.count != big_data.count) {
			throw Exception("Cardinalities of ORDER BY columns and input "
			                "columns don't match [?]");
		}

		// now perform the actual sort
		state->sorted_vector = unique_ptr<uint64_t[]>(new uint64_t[sort_collection.count]);
		sort_collection.Sort(description, state->sorted_vector.get());
	}

	if (state->position >= big_data.count) {
		return;
	}

	size_t remaining_data = min((size_t)STANDARD_VECTOR_SIZE, big_data.count - state->position);
	for (size_t i = 0; i < big_data.column_count(); i++) {
		chunk.data[i].count = remaining_data;
		for (size_t j = 0; j < remaining_data; j++) {
			chunk.data[i].SetValue(j, big_data.GetValue(i, state->sorted_vector[state->position + j]));
		}
	}
	state->position += STANDARD_VECTOR_SIZE;
}

unique_ptr<PhysicalOperatorState> PhysicalOrder::GetOperatorState(ExpressionExecutor *parent_executor) {
	return make_unique<PhysicalOrderOperatorState>(children[0].get(), parent_executor);
}
