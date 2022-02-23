#include "duckdb/function/aggregate/distributive_functions.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/types/null_value.hpp"
#include "duckdb/common/vector_operations/vector_operations.hpp"
#include "duckdb/common/algorithm.hpp"

namespace duckdb {

struct StringAggState {
	idx_t size;
	idx_t alloc_size;
	char *dataptr;
};

struct StringAggBaseFunction {
	template <class STATE>
	static void Initialize(STATE *state) {
		state->dataptr = nullptr;
		state->alloc_size = 0;
		state->size = 0;
	}

	template <class T, class STATE>
	static void Finalize(Vector &result, FunctionData *, STATE *state, T *target, ValidityMask &mask, idx_t idx) {
		if (!state->dataptr) {
			mask.SetInvalid(idx);
		} else {
			target[idx] = StringVector::AddString(result, state->dataptr, state->size);
		}
	}

	template <class STATE>
	static void Destroy(STATE *state) {
		if (state->dataptr) {
			delete[] state->dataptr;
		}
	}

	static bool IgnoreNull() {
		return true;
	}

	static inline void PerformOperation(StringAggState *state, const char *str, const char *sep, idx_t str_size,
	                                    idx_t sep_size) {
		if (!state->dataptr) {
			// first iteration: allocate space for the string and copy it into the state
			state->alloc_size = MaxValue<idx_t>(8, NextPowerOfTwo(str_size));
			state->dataptr = new char[state->alloc_size];
			state->size = str_size;
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
			state->size += str_size;
		}
	}

	static inline void PerformOperation(StringAggState *state, string_t str, string_t sep) {
		PerformOperation(state, str.GetDataUnsafe(), sep.GetDataUnsafe(), str.GetSize(), sep.GetSize());
	}

	static inline void PerformOperation(StringAggState *state, string_t str) {
		PerformOperation(state, str.GetDataUnsafe(), ",", str.GetSize(), 1);
	}
};

struct StringAggFunction : public StringAggBaseFunction {
	template <class A_TYPE, class B_TYPE, class STATE, class OP>
	static void Operation(STATE *state, FunctionData *bind_data, A_TYPE *str_data, B_TYPE *sep_data,
	                      ValidityMask &str_mask, ValidityMask &sep_mask, idx_t str_idx, idx_t sep_idx) {
		PerformOperation(state, str_data[str_idx], sep_data[sep_idx]);
	}
};

struct StringAggSingleFunction : public StringAggBaseFunction {
	template <class INPUT_TYPE, class STATE, class OP>
	static void Operation(STATE *state, FunctionData *bind_data, INPUT_TYPE *str_data, ValidityMask &str_mask,
	                      idx_t str_idx) {
		PerformOperation(state, str_data[str_idx]);
	}

	template <class INPUT_TYPE, class STATE, class OP>
	static void ConstantOperation(STATE *state, FunctionData *bind_data, INPUT_TYPE *input, ValidityMask &mask,
	                              idx_t count) {
		for (idx_t i = 0; i < count; i++) {
			Operation<INPUT_TYPE, STATE, OP>(state, bind_data, input, mask, 0);
		}
	}

	template <class STATE, class OP>
	static void Combine(const STATE &source, STATE *target) {
		if (!source.dataptr) {
			// source is not set: skip combining
			return;
		}
		PerformOperation(target, string_t(source.dataptr, source.size));
	}
};

void StringAggFun::RegisterFunction(BuiltinFunctions &set) {
	AggregateFunctionSet string_agg("string_agg");
	string_agg.AddFunction(AggregateFunction(
	    {LogicalType::VARCHAR, LogicalType::VARCHAR}, LogicalType::VARCHAR,
	    AggregateFunction::StateSize<StringAggState>,
	    AggregateFunction::StateInitialize<StringAggState, StringAggFunction>,
	    AggregateFunction::BinaryScatterUpdate<StringAggState, string_t, string_t, StringAggFunction>, nullptr,
	    AggregateFunction::StateFinalize<StringAggState, string_t, StringAggFunction>,
	    AggregateFunction::BinaryUpdate<StringAggState, string_t, string_t, StringAggFunction>, nullptr,
	    AggregateFunction::StateDestroy<StringAggState, StringAggFunction>, nullptr, nullptr));
	string_agg.AddFunction(
	    AggregateFunction({LogicalType::VARCHAR}, LogicalType::VARCHAR, AggregateFunction::StateSize<StringAggState>,
	                      AggregateFunction::StateInitialize<StringAggState, StringAggSingleFunction>,
	                      AggregateFunction::UnaryScatterUpdate<StringAggState, string_t, StringAggSingleFunction>,
	                      AggregateFunction::StateCombine<StringAggState, StringAggSingleFunction>,
	                      AggregateFunction::StateFinalize<StringAggState, string_t, StringAggSingleFunction>,
	                      AggregateFunction::UnaryUpdate<StringAggState, string_t, StringAggSingleFunction>, nullptr,
	                      AggregateFunction::StateDestroy<StringAggState, StringAggSingleFunction>, nullptr, nullptr));
	set.AddFunction(string_agg);
	string_agg.name = "group_concat";
	set.AddFunction(string_agg);
}

} // namespace duckdb
