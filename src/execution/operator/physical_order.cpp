
#include "execution/operator/physical_order.hpp"
#include "execution/expression_executor.hpp"

#include "execution/vector/vector_operations.hpp"

#include "storage/data_table.hpp"

using namespace duckdb;
using namespace std;

void PhysicalOrder::InitializeChunk(DataChunk &chunk) {
	// just copy the chunk data of the child
	children[0]->InitializeChunk(chunk);
}

int compare_tuple(DataChunk &sort_by, OrderByDescription &desc, size_t left,
                  size_t right) {
	for (size_t i = 0; i < desc.orders.size(); i++) {
		Value left_value = sort_by.data[i]->GetValue(left);
		Value right_value = sort_by.data[i]->GetValue(right);
		if (Value::Equals(left_value, right_value)) {
			continue;
		}
		auto order_type = desc.orders[i].type;
		return Value::LessThan(left_value, right_value)
		           ? (order_type == OrderType::ASCENDING ? -1 : 1)
		           : (order_type == OrderType::ASCENDING ? 1 : -1);
	}
	return 0;
}

static void _sort(DataChunk &sort_by, OrderByDescription &desc, sel_t *result) {
	// insertion sort
	result[0] = 0;
	for (size_t i = 1; i < sort_by.count; i++) {
		result[i] = i;
		for (size_t j = i; j > 0; j--) {
			if (compare_tuple(sort_by, desc, result[j - 1], i) < 0) {
				break;
			}
			swap(result[j], result[j - 1]);
		}
	}
}

void PhysicalOrder::GetChunk(DataChunk &chunk, PhysicalOperatorState *state_) {
	auto state = reinterpret_cast<PhysicalOrderOperatorState *>(state_);
	chunk.Reset();

	DataChunk &big_data = state->sorted_data;
	if (state->position == 0) {
		// first run of the order by: gather the data and sort
		InitializeChunk(big_data);

		// first concatenate all the data of the child chunks
		do {
			children[0]->GetChunk(state->child_chunk, state->child_state.get());
			big_data.Append(state->child_chunk);
		} while (state->child_chunk.count != 0);

		// now perform the actual ordering of the data
		// compute the sorting columns from the input data
		DataChunk sort_chunk;
		vector<TypeId> sort_types;
		for (size_t i = 0; i < description.orders.size(); i++) {
			auto &expr = description.orders[i].expression;
			sort_types.push_back(expr->return_type);
		}
		sort_chunk.Initialize(sort_types, big_data.count);

		ExpressionExecutor executor(big_data);
		for (size_t i = 0; i < description.orders.size(); i++) {
			auto &expr = description.orders[i].expression;
			executor.Execute(expr.get(), *sort_chunk.data[i]);
		}
		sort_chunk.count = sort_chunk.data[0]->count;

		if (sort_chunk.count != big_data.count) {
			throw Exception("Cardinalities of ORDER BY columns and input "
			                "columns don't match [?]");
		}

		// now perform the actual sort
		big_data.sel_vector = unique_ptr<sel_t[]>(new sel_t[sort_chunk.count]);
		_sort(sort_chunk, description, big_data.sel_vector.get());

		// now assign the selection vector to the children
		for (size_t i = 0; i < big_data.column_count; i++) {
			big_data.data[i]->sel_vector = big_data.sel_vector.get();
		}
	}

	if (state->position >= big_data.count) {
		return;
	}

	for (size_t i = 0; i < big_data.column_count; i++) {
		VectorOperations::Copy(*big_data.data[i].get(), *chunk.data[i].get(),
		                       state->position);
	}
	chunk.count = chunk.data[0]->count;
	state->position += chunk.count;
}

unique_ptr<PhysicalOperatorState> PhysicalOrder::GetOperatorState() {
	return make_unique<PhysicalOrderOperatorState>(children[0].get());
}
