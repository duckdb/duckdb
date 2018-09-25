
#include "execution/operator/physical_order.hpp"
#include "execution/expression_executor.hpp"

#include "common/types/vector_operations.hpp"

#include "storage/data_table.hpp"

#include "common/assert.hpp"

using namespace duckdb;
using namespace std;

vector<TypeId> PhysicalOrder::GetTypes() { return children[0]->GetTypes(); }

int compare_tuple(ChunkCollection &sort_by, OrderByDescription &desc,
                  size_t left, size_t right) {
	for (size_t i = 0; i < desc.orders.size(); i++) {
		Value left_value = sort_by.GetValue(i, left);
		Value right_value = sort_by.GetValue(i, right);
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

static int64_t _quicksort_initial(ChunkCollection &sort_by,
                                  OrderByDescription &desc, uint64_t *result) {
	// select pivot
	int64_t pivot = 0;
	int64_t low = 0, high = sort_by.count - 1;
	// now insert elements
	for (size_t i = 1; i < sort_by.count; i++) {
		if (compare_tuple(sort_by, desc, i, pivot) <= 0) {
			result[low++] = i;
		} else {
			result[high--] = i;
		}
	}
	assert(low == high);
	result[low] = pivot;
	return low;
}

static void _quicksort_inplace(ChunkCollection &sort_by,
                               OrderByDescription &desc, uint64_t *result,
                               int64_t left, int64_t right) {
	if (left >= right) {
		return;
	}

	int64_t middle = left + (right - left) / 2;
	int64_t pivot = result[middle];
	// move the mid point value to the front.
	int64_t i = left + 1;
	int64_t j = right;

	std::swap(result[middle], result[left]);
	while (i <= j) {
		while (i <= j && compare_tuple(sort_by, desc, result[i], pivot) <= 0) {
			i++;
		}

		while (i <= j && compare_tuple(sort_by, desc, result[j], pivot) > 0) {
			j--;
		}

		if (i < j) {
			std::swap(result[i], result[j]);
		}
	}
	std::swap(result[i - 1], result[left]);
	int64_t part = i - 1;

	_quicksort_inplace(sort_by, desc, result, left, part - 1);
	_quicksort_inplace(sort_by, desc, result, part + 1, right);
}

static void quicksort(ChunkCollection &sort_by, OrderByDescription &desc,
                      uint64_t *result) {
	if (sort_by.count == 0)
		return;
	// quicksort
	int64_t part = _quicksort_initial(sort_by, desc, result);
	_quicksort_inplace(sort_by, desc, result, 0, part);
	_quicksort_inplace(sort_by, desc, result, part + 1, sort_by.count - 1);
}

void PhysicalOrder::_GetChunk(ClientContext &context, DataChunk &chunk,
                              PhysicalOperatorState *state_) {
	auto state = reinterpret_cast<PhysicalOrderOperatorState *>(state_);
	chunk.Reset();

	ChunkCollection &big_data = state->sorted_data;
	if (state->position == 0) {
		// first concatenate all the data of the child chunks
		do {
			children[0]->GetChunk(context, state->child_chunk,
			                      state->child_state.get());
			big_data.Append(state->child_chunk);
		} while (state->child_chunk.count != 0);

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
			for (size_t i = 0; i < description.orders.size(); i++) {
				auto &expr = description.orders[i].expression;
				executor.Execute(expr.get(), sort_chunk.data[i]);
			}
			sort_chunk.count = sort_chunk.data[0].count;
			sort_collection.Append(sort_chunk);
		}

		if (sort_collection.count != big_data.count) {
			throw Exception("Cardinalities of ORDER BY columns and input "
			                "columns don't match [?]");
		}

		// now perform the actual sort
		state->sorted_vector =
		    unique_ptr<uint64_t[]>(new uint64_t[sort_collection.count]);
		quicksort(sort_collection, description, state->sorted_vector.get());
	}

	if (state->position >= big_data.count) {
		return;
	}

	size_t remaining_data =
	    min((size_t)STANDARD_VECTOR_SIZE, big_data.count - state->position);
	for (size_t i = 0; i < big_data.column_count(); i++) {
		chunk.data[i].count = remaining_data;
		for (size_t j = 0; j < remaining_data; j++) {
			chunk.data[i].SetValue(
			    j, big_data.GetValue(i, state->sorted_vector[j]));
		}
	}
	chunk.count = remaining_data;
	state->position += STANDARD_VECTOR_SIZE;
}

unique_ptr<PhysicalOperatorState>
PhysicalOrder::GetOperatorState(ExpressionExecutor *parent_executor) {
	return make_unique<PhysicalOrderOperatorState>(children[0].get(),
	                                               parent_executor);
}
