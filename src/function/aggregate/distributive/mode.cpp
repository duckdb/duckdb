// MODE( <expr1> )
// Returns the most frequent value for the values within expr1.
// NULL values are ignored. If all the values are NULL, or there are 0 rows, then the function returns NULL.

#include "duckdb/common/exception.hpp"
#include "duckdb/common/vector_operations/vector_operations.hpp"
#include "duckdb/function/aggregate/distributive_functions.hpp"
#include "duckdb/planner/expression/bound_aggregate_expression.hpp"

#include "duckdb/common/unordered_map.hpp"

namespace duckdb {

template <class KEY_TYPE>
struct ModeState {
	unordered_map<KEY_TYPE, size_t> *frequency_map;
};

template <typename KEY_TYPE>
struct ModeFunction {
	template <class STATE>
	static void Initialize(STATE *state) {
		state->frequency_map = nullptr;
	}

	template <class INPUT_TYPE, class STATE, class OP>
	static void Operation(STATE *state, FunctionData *bind_data, INPUT_TYPE *input, ValidityMask &mask, idx_t idx) {
		if (!state->frequency_map) {
			state->frequency_map = new unordered_map<KEY_TYPE, size_t>();
		}
		auto key = KEY_TYPE(input[idx]);
		(*state->frequency_map)[key]++;
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

	template <class INPUT_TYPE, class STATE>
	static void Finalize(Vector &result, FunctionData *, STATE *state, INPUT_TYPE *target, ValidityMask &mask,
	                     idx_t idx) {
		if (!state->frequency_map) {
			mask.SetInvalid(idx);
			return;
		}
		//! Initialize control variables to first variable of the frequency map
		auto highest_frequency = state->frequency_map->begin();
		for (auto i = highest_frequency; i != state->frequency_map->end(); ++i) {
			if (i->second > highest_frequency->second) {
				highest_frequency = i;
			}
		}
		target[idx] = INPUT_TYPE(highest_frequency->first);
	}
	template <class INPUT_TYPE, class STATE, class OP>
	static void ConstantOperation(STATE *state, FunctionData *bind_data, INPUT_TYPE *input, ValidityMask &mask,
	                              idx_t count) {
		for (idx_t i = 0; i < count; i++) {
			Operation<INPUT_TYPE, STATE, OP>(state, bind_data, input, mask, 0);
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

template <typename INPUT_TYPE, typename KEY_TYPE>
AggregateFunction GetTypedModeFunction(const LogicalType &type) {
	return AggregateFunction::UnaryAggregateDestructor<ModeState<KEY_TYPE>, INPUT_TYPE, INPUT_TYPE,
	                                                   ModeFunction<KEY_TYPE>>(type, type);
}

AggregateFunction GetModeFunction(const LogicalType &type) {
	switch (type.InternalType()) {
	case PhysicalType::INT16:
	case PhysicalType::UINT16:
		return GetTypedModeFunction<uint16_t, uint16_t>(type);
	case PhysicalType::INT32:
	case PhysicalType::UINT32:
		return GetTypedModeFunction<uint32_t, uint32_t>(type);
	case PhysicalType::INT64:
	case PhysicalType::UINT64:
		return GetTypedModeFunction<uint64_t, uint64_t>(type);

	case PhysicalType::FLOAT:
		return GetTypedModeFunction<float, float>(type);
	case PhysicalType::DOUBLE:
		return GetTypedModeFunction<double, double>(type);

	case PhysicalType::VARCHAR:
		return GetTypedModeFunction<string_t, string>(type);

	default:
		throw NotImplementedException("Unimplemented mode aggregate");
	}
}

void ModeFun::RegisterFunction(BuiltinFunctions &set) {
	AggregateFunctionSet fun("mode");
	fun.AddFunction(GetModeFunction(LogicalType::USMALLINT));
	fun.AddFunction(GetModeFunction(LogicalType::UINTEGER));
	fun.AddFunction(GetModeFunction(LogicalType::UBIGINT));
	fun.AddFunction(GetModeFunction(LogicalType::FLOAT));
	fun.AddFunction(GetModeFunction(LogicalType::SMALLINT));
	fun.AddFunction(GetModeFunction(LogicalType::INTEGER));
	fun.AddFunction(GetModeFunction(LogicalType::BIGINT));
	fun.AddFunction(GetModeFunction(LogicalType::DOUBLE));
	fun.AddFunction(GetModeFunction(LogicalType::VARCHAR));
	set.AddFunction(fun);
}
} // namespace duckdb
