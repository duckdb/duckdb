#include "duckdb/function/aggregate/distributive_functions.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/types/null_value.hpp"
#include "duckdb/common/vector_operations/vector_operations.hpp"
#include <string>

using namespace std;

namespace duckdb {

struct string_agg_state_t {
	char *dataptr;
	index_t size;
	index_t alloc_size;
};

static index_t string_agg_size(TypeId return_type) {
	return sizeof(string_agg_state_t);
}

static void string_agg_initialize(data_ptr_t state, TypeId return_type) {
	memset(state, 0, sizeof(string_agg_state_t));
}

static void string_agg_finalize(Vector &state, Vector &result) {
	// compute finalization of streaming avg
	auto states = (string_agg_state_t **)state.GetData();
	auto result_data = (string_t *)result.GetData();
	VectorOperations::Exec(state, [&](index_t i, index_t k) {
		auto state_ptr = states[i];

		if (state_ptr->dataptr == nullptr) {
			result.nullmask[i] = true;
		} else {
			result_data[i] = result.AddString(state_ptr->dataptr, state_ptr->size);
		}
	});
}

static void string_agg_update(Vector inputs[], index_t input_count, Vector &state) {
	assert(input_count == 2 && inputs[0].type == TypeId::VARCHAR && inputs[1].type == TypeId::VARCHAR);
	inputs[0].Normalify();
	inputs[1].Normalify();

	auto &strs = inputs[0];
	auto &seps = inputs[1];

	auto str_data = (string_t *)strs.GetData();
	auto sep_data = (string_t *)seps.GetData();
	auto states = (string_agg_state_t **)state.GetData();

	// Share a reusable buffer for the block
	VectorOperations::Exec(state, [&](index_t i, index_t k) {
		if (strs.nullmask[i] || seps.nullmask[i]) {
			return;
		}

		auto state_ptr = states[i];
		auto str = str_data[i].GetData();
		auto sep = sep_data[i].GetData();
		auto str_size = str_data[i].GetSize() + 1;
		auto sep_size = sep_data[i].GetSize();

		if (state_ptr->dataptr == nullptr) {
			// first iteration: allocate space for the string and copy it into the state
			state_ptr->alloc_size = std::max((index_t)8, (index_t)NextPowerOfTwo(str_size));
			state_ptr->dataptr = new char[state_ptr->alloc_size];
			state_ptr->size = str_size - 1;
			memcpy(state_ptr->dataptr, str, str_size);
		} else {
			// subsequent iteration: first check if we have space to place the string and separator
			index_t required_size = state_ptr->size + str_size + sep_size;
			if (required_size > state_ptr->alloc_size) {
				// no space! allocate extra space
				while (state_ptr->alloc_size < required_size) {
					state_ptr->alloc_size *= 2;
				}
				auto new_data = new char[state_ptr->alloc_size];
				memcpy(new_data, state_ptr->dataptr, state_ptr->size);
				delete[] state_ptr->dataptr;
				state_ptr->dataptr = new_data;
			}
			// copy the separator
			memcpy(state_ptr->dataptr + state_ptr->size, sep, sep_size);
			state_ptr->size += sep_size;
			// copy the string
			memcpy(state_ptr->dataptr + state_ptr->size, str, str_size);
			state_ptr->size += str_size - 1;
		}
	});
}

static void string_agg_destructor(Vector &state) {
	auto states = (string_agg_state_t **)state.GetData();
	VectorOperations::Exec(state, [&](index_t i, index_t k) {
		auto state_ptr = states[i];
		if (state_ptr->dataptr) {
			delete[] state_ptr->dataptr;
		}
	});
}

void StringAggFun::RegisterFunction(BuiltinFunctions &set) {
	set.AddFunction(AggregateFunction("string_agg", {SQLType::VARCHAR, SQLType::VARCHAR}, SQLType::VARCHAR,
	                                  string_agg_size, string_agg_initialize, string_agg_update, nullptr,
	                                  string_agg_finalize, nullptr, string_agg_destructor));
}

} // namespace duckdb
