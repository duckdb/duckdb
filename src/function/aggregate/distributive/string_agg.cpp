#include "duckdb/function/aggregate/distributive_functions.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/types/null_value.hpp"
#include "duckdb/common/vector_operations/vector_operations.hpp"
#include <string>

using namespace std;

namespace duckdb {

struct string_agg_state_t {
	char *dataptr;
	idx_t size;
	idx_t alloc_size;
};

struct StringAggFunction {
	template <class STATE> static void Initialize(STATE *state) {
		state->dataptr = nullptr;
		state->alloc_size = 0;
		state->size = 0;
	}

	template <class A_TYPE, class B_TYPE, class STATE, class OP>
	static void Operation(STATE *state, A_TYPE *str_data, B_TYPE *sep_data, nullmask_t &str_nullmask,
	                      nullmask_t &sep_nullmask, idx_t str_idx, idx_t sep_idx) {
		auto str = str_data[str_idx].GetData();
		auto sep = sep_data[sep_idx].GetData();
		auto str_size = str_data[str_idx].GetSize() + 1;
		auto sep_size = sep_data[sep_idx].GetSize();

		if (state->dataptr == nullptr) {
			// first iteration: allocate space for the string and copy it into the state
			state->alloc_size = std::max((idx_t)8, (idx_t)NextPowerOfTwo(str_size));
			state->dataptr = new char[state->alloc_size];
			state->size = str_size - 1;
			memcpy(state->dataptr, str, str_size);
		} else {
			// subsequent iteration: first check if we have space to place the string and separator
			idx_t required_size = state->size + str_size + sep_size;
			if (required_size > state->alloc_size) {
				// no space! allocate extra space
				while (state->alloc_size < required_size) {
					state->alloc_size *= 2;
				}
				auto new_data = new char[state->alloc_size];
				memcpy(new_data, state->dataptr, state->size);
				delete[] state->dataptr;
				state->dataptr = new_data;
			}
			// copy the separator
			memcpy(state->dataptr + state->size, sep, sep_size);
			state->size += sep_size;
			// copy the string
			memcpy(state->dataptr + state->size, str, str_size);
			state->size += str_size - 1;
		}
	}

	template <class STATE, class OP> static void Combine(STATE source, STATE *target) {
		throw NotImplementedException("String aggregate combine!");
	}

	template <class T, class STATE>
	static void Finalize(Vector &result, STATE *state, T *target, nullmask_t &nullmask, idx_t idx) {
		if (!state->dataptr) {
			nullmask[idx] = true;
		} else {
			target[idx] = StringVector::AddString(result, state->dataptr, state->size);
		}
	}

	template <class STATE> static void Destroy(STATE *state) {
		if (state->dataptr) {
			delete[] state->dataptr;
		}
	}

	static bool IgnoreNull() {
		return true;
	}
};

void StringAggFun::RegisterFunction(BuiltinFunctions &set) {
	AggregateFunctionSet string_agg("string_agg");
	string_agg.AddFunction(AggregateFunction::BinaryAggregateDestructor<string_agg_state_t, string_t, string_t,
	                                                                    string_t, StringAggFunction>(
	    SQLType::VARCHAR, SQLType::VARCHAR, SQLType::VARCHAR));
	set.AddFunction(string_agg);
}

} // namespace duckdb
