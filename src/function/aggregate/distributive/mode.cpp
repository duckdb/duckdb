// MODE( <expr1> )
// Returns the most frequent value for the values within expr1.
// NULL values are ignored. If all the values are NULL, or there are 0 rows, then the function returns NULL.

#include "duckdb/common/exception.hpp"
#include "duckdb/common/vector_operations/vector_operations.hpp"
#include "duckdb/function/aggregate/distributive_functions.hpp"
#include "duckdb/planner/expression/bound_aggregate_expression.hpp"

#include "duckdb/common/unordered_map.hpp"

namespace duckdb {

template <class T>
struct ModeState {
	unordered_map<T, size_t> *frequency_map;
};

struct ModeFunction {
	template <class STATE>
	static void Initialize(STATE *state) {
		state->frequency_map = nullptr;
	}

	template <class INPUT_TYPE, class STATE, class OP>
	static void Operation(STATE *state, FunctionData *bind_data, INPUT_TYPE *input, nullmask_t &nullmask, idx_t idx) {
		if (!state->frequency_map) {
			state->frequency_map = new unordered_map<INPUT_TYPE, size_t>();
		}
		(*state->frequency_map)[input[idx]]++;
	}

	template <class STATE, class OP>
	static void Combine(STATE &source, STATE *target) {
		if (!source.frequency_map) {
			return;
		}
		if (!target->frequency_map) {
			target->frequency_map = source.frequency_map;
			source.frequency_map = nullptr;
			return;
		}
		for (auto &val : *source.frequency_map) {
			(*target->frequency_map)[val.first] += val.second;
		}
	}

	template <class T, class STATE>
	static void Finalize(Vector &result, FunctionData *, STATE *state, T *target, nullmask_t &nullmask, idx_t idx) {
		if (!state->frequency_map) {
			nullmask[idx] = true;
			return;
		}
		T h_freq;
		size_t freq = 0;

		for (auto &val : *state->frequency_map) {
			if (val.second > freq) {
				h_freq = val.first;
				freq = val.second;
			}
		}
		target[idx] = h_freq;
	}
	template <class INPUT_TYPE, class STATE, class OP>
	static void ConstantOperation(STATE *state, FunctionData *bind_data, INPUT_TYPE *input, nullmask_t &nullmask,
	                              idx_t count) {
		for (idx_t i = 0; i < count; i++) {
			Operation<INPUT_TYPE, STATE, OP>(state, bind_data, input, nullmask, 0);
		}
	}

	static bool IgnoreNull() {
		return true;
	}

	template <class STATE>
	static void Destroy(STATE *state) {
		if (state->frequency_map) {
			delete state->frequency_map;
		}
	}
};

struct ModeFunctionString {
	template <class STATE>
	static void Initialize(STATE *state) {
		state->frequency_map = nullptr;
	}

	template <class INPUT_TYPE, class STATE, class OP>
	static void Operation(STATE *state, FunctionData *bind_data, INPUT_TYPE *input, nullmask_t &nullmask, idx_t idx) {
		if (!state->frequency_map) {
			state->frequency_map = new unordered_map<string, size_t>();
		}
		auto value = input[idx].GetString();
		(*state->frequency_map)[value]++;
	}

	template <class STATE, class OP>
	static void Combine(STATE &source, STATE *target) {
		if (!source.frequency_map) {
			return;
		}
		if (!target->frequency_map) {
			target->frequency_map = source.frequency_map;
			source.frequency_map = nullptr;
			return;
		}
		for (auto &val : *source.frequency_map) {
			auto value = val.first;
			(*target->frequency_map)[value] += val.second;
		}
	}

	template <class T, class STATE>
	static void Finalize(Vector &result, FunctionData *, STATE *state, T *target, nullmask_t &nullmask, idx_t idx) {
		if (!state->frequency_map) {
			nullmask[idx] = true;
			return;
		}
		T h_freq;
		size_t freq = 0;

		for (auto &val : *state->frequency_map) {
			if (val.second > freq) {
				h_freq = val.first;
				freq = val.second;
			}
		}
		target[idx] = string_t(h_freq);
	}
	template <class INPUT_TYPE, class STATE, class OP>
	static void ConstantOperation(STATE *state, FunctionData *bind_data, INPUT_TYPE *input, nullmask_t &nullmask,
	                              idx_t count) {
		for (idx_t i = 0; i < count; i++) {
			Operation<INPUT_TYPE, STATE, OP>(state, bind_data, input, nullmask, 0);
		}
	}

	static bool IgnoreNull() {
		return true;
	}

	template <class STATE>
	static void Destroy(STATE *state) {
		if (state->frequency_map) {
			delete state->frequency_map;
		}
	}
};

AggregateFunction GetModeFunction(PhysicalType type) {
	switch (type) {
	case PhysicalType::UINT16:
		return AggregateFunction::UnaryAggregateDestructor<ModeState<uint16_t>, uint16_t, uint16_t, ModeFunction>(
		    LogicalType::UTINYINT, LogicalType::UTINYINT);
	case PhysicalType::UINT32:
		return AggregateFunction::UnaryAggregateDestructor<ModeState<uint32_t>, uint32_t, uint32_t, ModeFunction>(
		    LogicalType::UINTEGER, LogicalType::UINTEGER);
	case PhysicalType::UINT64:
		return AggregateFunction::UnaryAggregateDestructor<ModeState<uint64_t>, uint64_t, uint64_t, ModeFunction>(
		    LogicalType::UBIGINT, LogicalType::UBIGINT);
	case PhysicalType::INT16:
		return AggregateFunction::UnaryAggregateDestructor<ModeState<int16_t>, int16_t, int16_t, ModeFunction>(
		    LogicalType::TINYINT, LogicalType::TINYINT);
	case PhysicalType::INT32:
		return AggregateFunction::UnaryAggregateDestructor<ModeState<int32_t>, int32_t, int32_t, ModeFunction>(
		    LogicalType::INTEGER, LogicalType::INTEGER);
	case PhysicalType::INT64:
		return AggregateFunction::UnaryAggregateDestructor<ModeState<int64_t>, int64_t, int64_t, ModeFunction>(
		    LogicalType::BIGINT, LogicalType::BIGINT);
	case PhysicalType::FLOAT:
		return AggregateFunction::UnaryAggregateDestructor<ModeState<float>, float, float, ModeFunction>(
		    LogicalType::FLOAT, LogicalType::FLOAT);
	case PhysicalType::DOUBLE:
		return AggregateFunction::UnaryAggregateDestructor<ModeState<double>, double, double, ModeFunction>(
		    LogicalType::DOUBLE, LogicalType::DOUBLE);
	case PhysicalType::VARCHAR:
		return AggregateFunction::UnaryAggregateDestructor<ModeState<string>, string_t, string_t, ModeFunctionString>(
		    LogicalType::VARCHAR, LogicalType::VARCHAR);

	default:
		throw NotImplementedException("Unimplemented mode aggregate");
	}
}

void ModeFun::RegisterFunction(BuiltinFunctions &set) {
	AggregateFunctionSet fun("mode");
	fun.AddFunction(GetModeFunction(PhysicalType::UINT16));
	fun.AddFunction(GetModeFunction(PhysicalType::UINT32));
	fun.AddFunction(GetModeFunction(PhysicalType::UINT64));
	fun.AddFunction(GetModeFunction(PhysicalType::FLOAT));
	fun.AddFunction(GetModeFunction(PhysicalType::INT16));
	fun.AddFunction(GetModeFunction(PhysicalType::INT32));
	fun.AddFunction(GetModeFunction(PhysicalType::INT64));
	fun.AddFunction(GetModeFunction(PhysicalType::DOUBLE));
	fun.AddFunction(GetModeFunction(PhysicalType::VARCHAR));
	set.AddFunction(fun);
}
} // namespace duckdb