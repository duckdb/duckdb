// MODE( <expr1> )
// Returns the most frequent value for the values within expr1.
// NULL values are ignored. If all the values are NULL, or there are 0 rows, then the function returns NULL.

#include "duckdb/common/exception.hpp"
#include "duckdb/common/vector_operations/vector_operations.hpp"
#include "duckdb/common/operator/comparison_operators.hpp"
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

using FrameBounds = std::pair<idx_t, idx_t>;

template <class KEY_TYPE>
struct ModeState {
	using Counts = unordered_map<KEY_TYPE, size_t>;

	Counts *frequency_map;
	KEY_TYPE *mode;
	size_t nonzero;
	bool valid;
	size_t count;

	void Initialize() {
		frequency_map = nullptr;
		mode = nullptr;
		nonzero = 0;
		valid = false;
		count = 0;
	}

	void Destroy() {
		if (frequency_map) {
			delete frequency_map;
		}
		if (mode) {
			delete mode;
		}
	}

	void Reset() {
		Counts empty;
		frequency_map->swap(empty);
		nonzero = 0;
		count = 0;
		valid = false;
	}

	void ModeAdd(const KEY_TYPE &key) {
		auto new_count = ((*frequency_map)[key] += 1);
		if (new_count == 1) {
			++nonzero;
		}
		if (new_count > count) {
			valid = true;
			count = new_count;
			if (mode) {
				*mode = key;
			} else {
				mode = new KEY_TYPE(key);
			}
		}
	}

	void ModeRm(const KEY_TYPE &key) {
		auto i = frequency_map->find(key);
		auto old_count = i->second;
		nonzero -= int(old_count == 1);

		i->second -= 1;
		if (count == old_count && key == *mode) {
			valid = false;
		}
	}

	typename Counts::const_iterator Scan() const {
		//! Initialize control variables to first variable of the frequency map
		auto highest_frequency = frequency_map->begin();
		for (auto i = highest_frequency; i != frequency_map->end(); ++i) {
			// Tie break with the lowest
			if (i->second > highest_frequency->second ||
			    (i->second == highest_frequency->second && i->first < highest_frequency->first)) {
				highest_frequency = i;
			}
		}
		return highest_frequency;
	}
};

struct ModeIncluded {
	inline explicit ModeIncluded(const ValidityMask &fmask_p, const ValidityMask &dmask_p, idx_t bias_p)
	    : fmask(fmask_p), dmask(dmask_p), bias(bias_p) {
	}

	inline bool operator()(const idx_t &idx) const {
		return fmask.RowIsValid(idx) && dmask.RowIsValid(idx - bias);
	}
	const ValidityMask &fmask;
	const ValidityMask &dmask;
	const idx_t bias;
};

template <typename KEY_TYPE>
struct ModeFunction {
	template <class STATE>
	static void Initialize(STATE *state) {
		state->Initialize();
	}

	template <class INPUT_TYPE, class STATE, class OP>
	static void Operation(STATE *state, AggregateInputData &, INPUT_TYPE *input, ValidityMask &mask, idx_t idx) {
		if (!state->frequency_map) {
			state->frequency_map = new unordered_map<KEY_TYPE, size_t>();
		}
		auto key = KEY_TYPE(input[idx]);
		(*state->frequency_map)[key]++;
	}

	template <class STATE, class OP>
	static void Combine(const STATE &source, STATE *target, AggregateInputData &) {
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
	static void Finalize(Vector &result, AggregateInputData &, STATE *state, INPUT_TYPE *target, ValidityMask &mask,
	                     idx_t idx) {
		if (!state->frequency_map) {
			mask.SetInvalid(idx);
			return;
		}
		auto highest_frequency = state->Scan();
		if (highest_frequency != state->frequency_map->end()) {
			target[idx] = INPUT_TYPE(highest_frequency->first);
		} else {
			mask.SetInvalid(idx);
		}
	}
	template <class INPUT_TYPE, class STATE, class OP>
	static void ConstantOperation(STATE *state, AggregateInputData &, INPUT_TYPE *input, ValidityMask &mask,
	                              idx_t count) {
		if (!state->frequency_map) {
			state->frequency_map = new unordered_map<KEY_TYPE, size_t>();
		}
		auto key = KEY_TYPE(input[0]);
		(*state->frequency_map)[key] += count;
	}

	template <class STATE, class INPUT_TYPE, class RESULT_TYPE>
	static void Window(const INPUT_TYPE *data, const ValidityMask &fmask, const ValidityMask &dmask,
	                   AggregateInputData &, STATE *state, const FrameBounds &frame, const FrameBounds &prev,
	                   Vector &result, idx_t rid, idx_t bias) {
		auto rdata = FlatVector::GetData<RESULT_TYPE>(result);
		auto &rmask = FlatVector::Validity(result);

		ModeIncluded included(fmask, dmask, bias);

		if (!state->frequency_map) {
			state->frequency_map = new unordered_map<KEY_TYPE, size_t>();
		}
		const double tau = .25;
		if (state->nonzero <= tau * state->frequency_map->size()) {
			state->Reset();
			// for f ∈ F do
			for (auto f = frame.first; f < frame.second; ++f) {
				if (included(f)) {
					state->ModeAdd(KEY_TYPE(data[f]));
				}
			}
		} else {
			// for f ∈ P \ F do
			for (auto p = prev.first; p < frame.first; ++p) {
				if (included(p)) {
					state->ModeRm(KEY_TYPE(data[p]));
				}
			}
			for (auto p = frame.second; p < prev.second; ++p) {
				if (included(p)) {
					state->ModeRm(KEY_TYPE(data[p]));
				}
			}

			// for f ∈ F \ P do
			for (auto f = frame.first; f < prev.first; ++f) {
				if (included(f)) {
					state->ModeAdd(KEY_TYPE(data[f]));
				}
			}
			for (auto f = prev.second; f < frame.second; ++f) {
				if (included(f)) {
					state->ModeAdd(KEY_TYPE(data[f]));
				}
			}
		}

		if (!state->valid) {
			// Rescan
			auto highest_frequency = state->Scan();
			if (highest_frequency != state->frequency_map->end()) {
				*(state->mode) = highest_frequency->first;
				state->count = highest_frequency->second;
				state->valid = (state->count > 0);
			}
		}

		if (state->valid) {
			rdata[rid] = RESULT_TYPE(*state->mode);
		} else {
			rmask.Set(rid, false);
		}
	}

	static bool IgnoreNull() {
		return true;
	}

	template <class STATE>
	static void Destroy(STATE *state) {
		state->Destroy();
	}
};

template <typename INPUT_TYPE, typename KEY_TYPE>
AggregateFunction GetTypedModeFunction(const LogicalType &type) {
	using STATE = ModeState<KEY_TYPE>;
	using OP = ModeFunction<KEY_TYPE>;
	auto func = AggregateFunction::UnaryAggregateDestructor<STATE, INPUT_TYPE, INPUT_TYPE, OP>(type, type);
	func.window = AggregateFunction::UnaryWindow<STATE, INPUT_TYPE, INPUT_TYPE, OP>;
	return func;
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
	const vector<LogicalType> TEMPORAL = {LogicalType::DATE,         LogicalType::TIMESTAMP, LogicalType::TIME,
	                                      LogicalType::TIMESTAMP_TZ, LogicalType::TIME_TZ,   LogicalType::INTERVAL};

	AggregateFunctionSet mode("mode");
	mode.AddFunction(AggregateFunction({LogicalTypeId::DECIMAL}, LogicalTypeId::DECIMAL, nullptr, nullptr, nullptr,
	                                   nullptr, nullptr, nullptr, BindModeDecimal));

	for (const auto &type : LogicalType::Numeric()) {
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
