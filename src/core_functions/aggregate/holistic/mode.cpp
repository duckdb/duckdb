// MODE( <expr1> )
// Returns the most frequent value for the values within expr1.
// NULL values are ignored. If all the values are NULL, or there are 0 rows, then the function returns NULL.

#include "duckdb/common/exception.hpp"
#include "duckdb/common/uhugeint.hpp"
#include "duckdb/common/vector_operations/vector_operations.hpp"
#include "duckdb/common/operator/comparison_operators.hpp"
#include "duckdb/core_functions/aggregate/holistic_functions.hpp"
#include "duckdb/planner/expression/bound_aggregate_expression.hpp"
#include "duckdb/common/unordered_map.hpp"
#include "duckdb/common/owning_string_map.hpp"
#include <functional>

namespace std {

template <>
struct hash<duckdb::interval_t> {
	inline size_t operator()(const duckdb::interval_t &val) const {
		int64_t months, days, micros;
		val.Normalize(months, days, micros);
		return hash<int32_t> {}(duckdb::UnsafeNumericCast<int32_t>(days)) ^
		       hash<int32_t> {}(duckdb::UnsafeNumericCast<int32_t>(months)) ^ hash<int64_t> {}(micros);
	}
};

template <>
struct hash<duckdb::hugeint_t> {
	inline size_t operator()(const duckdb::hugeint_t &val) const {
		return hash<int64_t> {}(val.upper) ^ hash<uint64_t> {}(val.lower);
	}
};

template <>
struct hash<duckdb::uhugeint_t> {
	inline size_t operator()(const duckdb::uhugeint_t &val) const {
		return hash<uint64_t> {}(val.upper) ^ hash<uint64_t> {}(val.lower);
	}
};

} // namespace std

namespace duckdb {

struct ModeAttr {
	ModeAttr() : count(0), first_row(std::numeric_limits<idx_t>::max()) {
	}
	size_t count;
	idx_t first_row;
};

template <class T>
struct ModeStandard {
	using MAP_TYPE = unordered_map<T, ModeAttr>;

	static MAP_TYPE *CreateEmpty(ArenaAllocator &) {
		return new MAP_TYPE();
	}
	static MAP_TYPE *CreateEmpty(Allocator &) {
		return new MAP_TYPE();
	}

	template <class INPUT_TYPE, class RESULT_TYPE>
	static RESULT_TYPE Assign(Vector &result, INPUT_TYPE input) {
		return RESULT_TYPE(input);
	}
};

struct ModeString {
	using MAP_TYPE = OwningStringMap<ModeAttr>;

	static MAP_TYPE *CreateEmpty(ArenaAllocator &allocator) {
		return new MAP_TYPE(allocator);
	}
	static MAP_TYPE *CreateEmpty(Allocator &allocator) {
		return new MAP_TYPE(allocator);
	}

	template <class INPUT_TYPE, class RESULT_TYPE>
	static RESULT_TYPE Assign(Vector &result, INPUT_TYPE input) {
		return StringVector::AddString(result, input);
	}
};

template <class KEY_TYPE, class TYPE_OP>
struct ModeState {
	using Counts = typename TYPE_OP::MAP_TYPE;

	ModeState() {
	}

	SubFrames prevs;
	Counts *frequency_map = nullptr;
	KEY_TYPE *mode = nullptr;
	size_t nonzero = 0;
	bool valid = false;
	size_t count = 0;

	~ModeState() {
		if (frequency_map) {
			delete frequency_map;
		}
		if (mode) {
			delete mode;
		}
	}

	void Reset() {
		if (frequency_map) {
			frequency_map->clear();
		}
		nonzero = 0;
		count = 0;
		valid = false;
	}

	void ModeAdd(const KEY_TYPE &key, idx_t row) {
		auto &attr = (*frequency_map)[key];
		auto new_count = (attr.count += 1);
		if (new_count == 1) {
			++nonzero;
			attr.first_row = row;
		} else {
			attr.first_row = MinValue(row, attr.first_row);
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

	void ModeRm(const KEY_TYPE &key, idx_t frame) {
		auto &attr = (*frequency_map)[key];
		auto old_count = attr.count;
		nonzero -= size_t(old_count == 1);

		attr.count -= 1;
		if (count == old_count && key == *mode) {
			valid = false;
		}
	}

	typename Counts::const_iterator Scan() const {
		//! Initialize control variables to first variable of the frequency map
		auto highest_frequency = frequency_map->begin();
		for (auto i = highest_frequency; i != frequency_map->end(); ++i) {
			// Tie break with the lowest insert position
			if (i->second.count > highest_frequency->second.count ||
			    (i->second.count == highest_frequency->second.count &&
			     i->second.first_row < highest_frequency->second.first_row)) {
				highest_frequency = i;
			}
		}
		return highest_frequency;
	}
};

struct ModeIncluded {
	inline explicit ModeIncluded(const ValidityMask &fmask_p, const ValidityMask &dmask_p)
	    : fmask(fmask_p), dmask(dmask_p) {
	}

	inline bool operator()(const idx_t &idx) const {
		return fmask.RowIsValid(idx) && dmask.RowIsValid(idx);
	}
	const ValidityMask &fmask;
	const ValidityMask &dmask;
};

template <typename TYPE_OP>
struct ModeFunction {
	template <class STATE>
	static void Initialize(STATE &state) {
		new (&state) STATE();
	}

	template <class INPUT_TYPE, class STATE, class OP>
	static void Operation(STATE &state, const INPUT_TYPE &key, AggregateUnaryInput &aggr_input) {
		if (!state.frequency_map) {
			state.frequency_map = TYPE_OP::CreateEmpty(aggr_input.input.allocator);
		}
		auto &i = (*state.frequency_map)[key];
		i.count++;
		i.first_row = MinValue<idx_t>(i.first_row, state.count);
		state.count++;
	}

	template <class STATE, class OP>
	static void Combine(const STATE &source, STATE &target, AggregateInputData &) {
		if (!source.frequency_map) {
			return;
		}
		if (!target.frequency_map) {
			// Copy - don't destroy! Otherwise windowing will break.
			target.frequency_map = new typename STATE::Counts(*source.frequency_map);
			return;
		}
		for (auto &val : *source.frequency_map) {
			auto &i = (*target.frequency_map)[val.first];
			i.count += val.second.count;
			i.first_row = MinValue(i.first_row, val.second.first_row);
		}
		target.count += source.count;
	}

	template <class T, class STATE>
	static void Finalize(STATE &state, T &target, AggregateFinalizeData &finalize_data) {
		if (!state.frequency_map) {
			finalize_data.ReturnNull();
			return;
		}
		auto highest_frequency = state.Scan();
		if (highest_frequency != state.frequency_map->end()) {
			target = TYPE_OP::template Assign<T, T>(finalize_data.result, highest_frequency->first);
		} else {
			finalize_data.ReturnNull();
		}
	}
	template <class INPUT_TYPE, class STATE, class OP>
	static void ConstantOperation(STATE &state, const INPUT_TYPE &key, AggregateUnaryInput &aggr_input, idx_t count) {
		if (!state.frequency_map) {
			state.frequency_map = TYPE_OP::CreateEmpty(aggr_input.input.allocator);
		}
		auto &i = (*state.frequency_map)[key];
		i.count += count;
		i.first_row = MinValue<idx_t>(i.first_row, state.count);
		state.count += count;
	}

	template <typename STATE, typename INPUT_TYPE>
	struct UpdateWindowState {
		STATE &state;
		const INPUT_TYPE *data;
		ModeIncluded &included;

		inline UpdateWindowState(STATE &state, const INPUT_TYPE *data, ModeIncluded &included)
		    : state(state), data(data), included(included) {
		}

		inline void Neither(idx_t begin, idx_t end) {
		}

		inline void Left(idx_t begin, idx_t end) {
			for (; begin < end; ++begin) {
				if (included(begin)) {
					state.ModeRm(data[begin], begin);
				}
			}
		}

		inline void Right(idx_t begin, idx_t end) {
			for (; begin < end; ++begin) {
				if (included(begin)) {
					state.ModeAdd(data[begin], begin);
				}
			}
		}

		inline void Both(idx_t begin, idx_t end) {
		}
	};

	template <class STATE, class INPUT_TYPE, class RESULT_TYPE>
	static void Window(const INPUT_TYPE *data, const ValidityMask &fmask, const ValidityMask &dmask,
	                   AggregateInputData &aggr_input_data, STATE &state, const SubFrames &frames, Vector &result,
	                   idx_t rid, const STATE *gstate) {
		auto rdata = FlatVector::GetData<RESULT_TYPE>(result);
		auto &rmask = FlatVector::Validity(result);
		auto &prevs = state.prevs;
		if (prevs.empty()) {
			prevs.resize(1);
		}

		ModeIncluded included(fmask, dmask);

		if (!state.frequency_map) {
			state.frequency_map = TYPE_OP::CreateEmpty(Allocator::DefaultAllocator());
		}
		const size_t tau_inverse = 4; // tau==0.25
		if (state.nonzero <= (state.frequency_map->size() / tau_inverse) || prevs.back().end <= frames.front().start ||
		    frames.back().end <= prevs.front().start) {
			state.Reset();
			// for f ∈ F do
			for (const auto &frame : frames) {
				for (auto i = frame.start; i < frame.end; ++i) {
					if (included(i)) {
						state.ModeAdd(data[i], i);
					}
				}
			}
		} else {
			using Updater = UpdateWindowState<STATE, INPUT_TYPE>;
			Updater updater(state, data, included);
			AggregateExecutor::IntersectFrames(prevs, frames, updater);
		}

		if (!state.valid) {
			// Rescan
			auto highest_frequency = state.Scan();
			if (highest_frequency != state.frequency_map->end()) {
				*(state.mode) = highest_frequency->first;
				state.count = highest_frequency->second.count;
				state.valid = (state.count > 0);
			}
		}

		if (state.valid) {
			rdata[rid] = TYPE_OP::template Assign<INPUT_TYPE, RESULT_TYPE>(result, *state.mode);
		} else {
			rmask.Set(rid, false);
		}

		prevs = frames;
	}

	static bool IgnoreNull() {
		return true;
	}

	template <class STATE>
	static void Destroy(STATE &state, AggregateInputData &aggr_input_data) {
		state.~STATE();
	}
};

template <typename INPUT_TYPE, typename TYPE_OP = ModeStandard<INPUT_TYPE>>
AggregateFunction GetTypedModeFunction(const LogicalType &type) {
	using STATE = ModeState<INPUT_TYPE, TYPE_OP>;
	using OP = ModeFunction<TYPE_OP>;
	auto return_type = type.id() == LogicalTypeId::ANY ? LogicalType::VARCHAR : type;
	auto func = AggregateFunction::UnaryAggregateDestructor<STATE, INPUT_TYPE, INPUT_TYPE, OP>(type, return_type);
	func.window = AggregateFunction::UnaryWindow<STATE, INPUT_TYPE, INPUT_TYPE, OP>;
	return func;
}

AggregateFunction GetModeAggregate(const LogicalType &type) {
	switch (type.InternalType()) {
	case PhysicalType::INT8:
		return GetTypedModeFunction<int8_t>(type);
	case PhysicalType::UINT8:
		return GetTypedModeFunction<uint8_t>(type);
	case PhysicalType::INT16:
		return GetTypedModeFunction<int16_t>(type);
	case PhysicalType::UINT16:
		return GetTypedModeFunction<uint16_t>(type);
	case PhysicalType::INT32:
		return GetTypedModeFunction<int32_t>(type);
	case PhysicalType::UINT32:
		return GetTypedModeFunction<uint32_t>(type);
	case PhysicalType::INT64:
		return GetTypedModeFunction<int64_t>(type);
	case PhysicalType::UINT64:
		return GetTypedModeFunction<uint64_t>(type);
	case PhysicalType::INT128:
		return GetTypedModeFunction<hugeint_t>(type);
	case PhysicalType::UINT128:
		return GetTypedModeFunction<uhugeint_t>(type);
	case PhysicalType::FLOAT:
		return GetTypedModeFunction<float>(type);
	case PhysicalType::DOUBLE:
		return GetTypedModeFunction<double>(type);
	case PhysicalType::INTERVAL:
		return GetTypedModeFunction<interval_t>(type);
	case PhysicalType::VARCHAR:
		return GetTypedModeFunction<string_t, ModeString>(LogicalType::ANY_PARAMS(LogicalType::VARCHAR, 150));
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

AggregateFunctionSet ModeFun::GetFunctions() {
	const vector<LogicalType> TEMPORAL = {LogicalType::DATE,         LogicalType::TIMESTAMP, LogicalType::TIME,
	                                      LogicalType::TIMESTAMP_TZ, LogicalType::TIME_TZ,   LogicalType::INTERVAL};

	AggregateFunctionSet mode;
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
	return mode;
}
} // namespace duckdb
