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

	bool IsEmpty() {
		return dataptr == nullptr;
	}

	void AddString(string_t str) {
		auto str_len = str.GetSize();
		if (IsEmpty()) {
			// first iteration: allocate space for the string and copy it into the state
			alloc_size = std::max((index_t) 16, (index_t) NextPowerOfTwo(str_len));
			dataptr = new char[alloc_size];
		} else {
			// subsequent iteration: first check if we have space to place the string and separator
			index_t required_size = size + str_len + 1;
			if (required_size > alloc_size) {
				// no space! allocate extra space
				while(alloc_size < required_size) {
					alloc_size *= 2;
				}
				auto new_data = new char[alloc_size];
				memcpy(new_data, dataptr, size);
				delete [] dataptr;
				dataptr = new_data;
			}
		}
		memcpy(dataptr + size, str.GetData(), str_len);
		size += str_len;
		dataptr[size] = '\0';
	}
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
		auto str = str_data[i];
		auto sep = sep_data[i];

		if (state_ptr->IsEmpty()) {
			state_ptr->AddString(str);
		} else {
			state_ptr->AddString(sep);
			state_ptr->AddString(str);
		}
	});
}

static void string_agg_destructor(Vector &state) {
	auto states = (string_agg_state_t **)state.GetData();
	VectorOperations::Exec(state, [&](index_t i, index_t k) {
		auto state_ptr = states[i];
		if (state_ptr->dataptr) {
			delete [] state_ptr->dataptr;
		}
	});
}

void StringAggFun::RegisterFunction(BuiltinFunctions &set) {
	set.AddFunction(AggregateFunction("string_agg", {SQLType::VARCHAR, SQLType::VARCHAR}, SQLType::VARCHAR,
	                                  string_agg_size, string_agg_initialize, string_agg_update, nullptr,
	                                  string_agg_finalize, nullptr, string_agg_destructor));
}

} // namespace duckdb
