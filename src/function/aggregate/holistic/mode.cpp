// MODE( <expr1> )
// Returns the most frequent value for the values within expr1.
// NULL values are ignored. If all the values are NULL, or there are 0 rows, then the function returns NULL.

#include "duckdb/common/exception.hpp"
#include "duckdb/common/vector_operations/vector_operations.hpp"
#include "duckdb/function/aggregate/holistic_functions.hpp"
#include "duckdb/planner/expression/bound_aggregate_expression.hpp"

#include "duckdb/common/unordered_map.hpp"

#include <functional>

namespace std {

template <>
struct hash<duckdb::interval_t> {
	inline size_t operator()(const duckdb::interval_t &val) const {
		return hash<int32_t> {}(val.days) ^ hash<int32_t> {}(val.months) ^ hash<int64_t> {}(val.micros);
	}
};

template <>
struct hash<duckdb::hugeint_t> {
	inline size_t operator()(const duckdb::hugeint_t &val) const {
		return hash<int64_t> {}(val.upper) ^ hash<int64_t> {}(val.lower);
	}
};

} // namespace std

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
			// Copy - don't destroy! Otherwise windowing will break.
			target->frequency_map = new unordered_map<KEY_TYPE, size_t>(*source.frequency_map);
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

AggregateFunction GetModeAggregate(const LogicalType &type) {
	switch (type.InternalType()) {
	case PhysicalType::INT8:
		return GetTypedModeFunction<int8_t, int8_t>(type);
	case PhysicalType::UINT8:
		return GetTypedModeFunction<uint8_t, uint8_t>(type);
	case PhysicalType::INT16:
		return GetTypedModeFunction<int16_t, int16_t>(type);
	case PhysicalType::UINT16:
		return GetTypedModeFunction<uint16_t, uint16_t>(type);
	case PhysicalType::INT32:
		return GetTypedModeFunction<int32_t, int32_t>(type);
	case PhysicalType::UINT32:
		return GetTypedModeFunction<uint32_t, uint32_t>(type);
	case PhysicalType::INT64:
		return GetTypedModeFunction<int64_t, int64_t>(type);
	case PhysicalType::UINT64:
		return GetTypedModeFunction<uint64_t, uint64_t>(type);
	case PhysicalType::INT128:
		return GetTypedModeFunction<hugeint_t, hugeint_t>(type);

	case PhysicalType::FLOAT:
		return GetTypedModeFunction<float, float>(type);
	case PhysicalType::DOUBLE:
		return GetTypedModeFunction<double, double>(type);

	case PhysicalType::INTERVAL:
		return GetTypedModeFunction<interval_t, interval_t>(type);

	case PhysicalType::VARCHAR:
		return GetTypedModeFunction<string_t, string>(type);

	default:
		throw NotImplementedException("Unimplemented mode aggregate");
	}
}

unique_ptr<FunctionData> BindModeDecimal(ClientContext &context, AggregateFunction &function,
                                         vector<unique_ptr<Expression>> &arguments) {
	function = GetModeAggregate(arguments[0]->return_type);
	function.name = "mode";
	return nullptr;
}

void ModeFun::RegisterFunction(BuiltinFunctions &set) {
	const vector<LogicalType> TEMPORAL = {LogicalType::DATE, LogicalType::TIMESTAMP, LogicalType::TIME,
	                                      LogicalType::INTERVAL};

	AggregateFunctionSet mode("mode");
	mode.AddFunction(AggregateFunction({LogicalType::DECIMAL}, LogicalType::DECIMAL, nullptr, nullptr, nullptr, nullptr,
	                                   nullptr, nullptr, BindModeDecimal));

	for (const auto &type : LogicalType::NUMERIC) {
		if (type.id() != LogicalTypeId::DECIMAL) {
			mode.AddFunction(GetModeAggregate(type));
		}
	}

	for (const auto &type : TEMPORAL) {
		mode.AddFunction(GetModeAggregate(type));
	}

	mode.AddFunction(GetModeAggregate(LogicalType::VARCHAR));

	set.AddFunction(mode);
}
} // namespace duckdb
