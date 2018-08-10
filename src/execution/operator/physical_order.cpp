
#include "execution/operator/physical_order.hpp"
#include "execution/expression_executor.hpp"

#include "common/types/vector_operations.hpp"

#include "storage/data_table.hpp"

#include "common/assert.hpp"

using namespace duckdb;
using namespace std;

vector<TypeId> PhysicalOrder::GetTypes() { return children[0]->GetTypes(); }

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

static void insertion_sort(DataChunk &sort_by, OrderByDescription &desc,
                           sel_t *result) {
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

static int64_t _quicksort_initial(DataChunk &sort_by, OrderByDescription &desc,
                                  sel_t *result) {
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

static void _quicksort_inplace(DataChunk &sort_by, OrderByDescription &desc,
                               sel_t *result, int64_t left, int64_t right) {
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

static void quicksort(DataChunk &sort_by, OrderByDescription &desc,
                      sel_t *result) {
	if (sort_by.count == 0)
		return;
	// quicksort
	int64_t part = _quicksort_initial(sort_by, desc, result);
	_quicksort_inplace(sort_by, desc, result, 0, part);
	_quicksort_inplace(sort_by, desc, result, part + 1, sort_by.count - 1);
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
		quicksort(sort_chunk, description, big_data.sel_vector.get());

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
