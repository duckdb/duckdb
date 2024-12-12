#include "duckdb/common/exception.hpp"
#include "duckdb/common/uhugeint.hpp"
#include "duckdb/common/vector_operations/vector_operations.hpp"
#include "duckdb/common/operator/comparison_operators.hpp"
#include "duckdb/common/types/column/column_data_collection.hpp"
#include "core_functions/aggregate/distributive_functions.hpp"
#include "core_functions/aggregate/holistic_functions.hpp"
#include "duckdb/planner/expression/bound_aggregate_expression.hpp"
#include "duckdb/common/unordered_map.hpp"
#include "duckdb/common/owning_string_map.hpp"
#include "duckdb/function/create_sort_key.hpp"
#include "duckdb/function/aggregate/sort_key_helpers.hpp"
#include "duckdb/common/algorithm.hpp"
#include <functional>

// MODE( <expr1> )
// Returns the most frequent value for the values within expr1.
// NULL values are ignored. If all the values are NULL, or there are 0 rows, then the function returns NULL.

namespace std {} // namespace std

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
		return StringVector::AddStringOrBlob(result, input);
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

	//! The collection being read
	const ColumnDataCollection *inputs;
	//! The state used for reading the collection on this thread
	ColumnDataScanState *scan = nullptr;
	//! The data chunk paged into into
	DataChunk page;
	//! The data pointer
	const KEY_TYPE *data = nullptr;
	//! The validity mask
	const ValidityMask *validity = nullptr;

	~ModeState() {
		if (frequency_map) {
			delete frequency_map;
		}
		if (mode) {
			delete mode;
		}
		if (scan) {
			delete scan;
		}
	}

	void InitializePage(const WindowPartitionInput &partition) {
		if (!scan) {
			scan = new ColumnDataScanState();
		}
		if (page.ColumnCount() == 0) {
			D_ASSERT(partition.inputs);
			inputs = partition.inputs;
			D_ASSERT(partition.column_ids.size() == 1);
			inputs->InitializeScan(*scan, partition.column_ids);
			inputs->InitializeScanChunk(*scan, page);
		}
	}

	inline sel_t RowOffset(idx_t row_idx) const {
		D_ASSERT(RowIsVisible(row_idx));
		return UnsafeNumericCast<sel_t>(row_idx - scan->current_row_index);
	}

	inline bool RowIsVisible(idx_t row_idx) const {
		return (row_idx < scan->next_row_index && scan->current_row_index <= row_idx);
	}

	inline idx_t Seek(idx_t row_idx) {
		if (!RowIsVisible(row_idx)) {
			D_ASSERT(inputs);
			inputs->Seek(row_idx, *scan, page);
			data = FlatVector::GetData<KEY_TYPE>(page.data[0]);
			validity = &FlatVector::Validity(page.data[0]);
		}
		return RowOffset(row_idx);
	}

	inline const KEY_TYPE &GetCell(idx_t row_idx) {
		const auto offset = Seek(row_idx);
		return data[offset];
	}

	inline bool RowIsValid(idx_t row_idx) {
		const auto offset = Seek(row_idx);
		return validity->RowIsValid(offset);
	}

	void Reset() {
		if (frequency_map) {
			frequency_map->clear();
		}
		nonzero = 0;
		count = 0;
		valid = false;
	}

	void ModeAdd(idx_t row) {
		const auto &key = GetCell(row);
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

	void ModeRm(idx_t frame) {
		const auto &key = GetCell(frame);
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

template <typename STATE>
struct ModeIncluded {
	inline explicit ModeIncluded(const ValidityMask &fmask_p, STATE &state) : fmask(fmask_p), state(state) {
	}

	inline bool operator()(const idx_t &idx) const {
		return fmask.RowIsValid(idx) && state.RowIsValid(idx);
	}
	const ValidityMask &fmask;
	STATE &state;
};

template <typename TYPE_OP>
struct BaseModeFunction {
	template <class STATE>
	static void Initialize(STATE &state) {
		new (&state) STATE();
	}

	template <class INPUT_TYPE, class STATE, class OP>
	static void Execute(STATE &state, const INPUT_TYPE &key, AggregateInputData &input_data) {
		if (!state.frequency_map) {
			state.frequency_map = TYPE_OP::CreateEmpty(input_data.allocator);
		}
		auto &i = (*state.frequency_map)[key];
		++i.count;
		i.first_row = MinValue<idx_t>(i.first_row, state.count);
		++state.count;
	}

	template <class INPUT_TYPE, class STATE, class OP>
	static void Operation(STATE &state, const INPUT_TYPE &key, AggregateUnaryInput &aggr_input) {
		Execute<INPUT_TYPE, STATE, OP>(state, key, aggr_input.input);
	}

	template <class STATE, class OP>
	static void Combine(const STATE &source, STATE &target, AggregateInputData &) {
		if (!source.frequency_map) {
			return;
		}
		if (!target.frequency_map) {
			// Copy - don't destroy! Otherwise windowing will break.
			target.frequency_map = new typename STATE::Counts(*source.frequency_map);
			target.count = source.count;
			return;
		}
		for (auto &val : *source.frequency_map) {
			auto &i = (*target.frequency_map)[val.first];
			i.count += val.second.count;
			i.first_row = MinValue(i.first_row, val.second.first_row);
		}
		target.count += source.count;
	}

	static bool IgnoreNull() {
		return true;
	}

	template <class STATE>
	static void Destroy(STATE &state, AggregateInputData &aggr_input_data) {
		state.~STATE();
	}
};

template <typename TYPE_OP>
struct TypedModeFunction : BaseModeFunction<TYPE_OP> {
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
};

template <typename TYPE_OP>
struct ModeFunction : TypedModeFunction<TYPE_OP> {
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

	template <typename STATE, typename INPUT_TYPE>
	struct UpdateWindowState {
		STATE &state;
		ModeIncluded<STATE> &included;

		inline UpdateWindowState(STATE &state, ModeIncluded<STATE> &included) : state(state), included(included) {
		}

		inline void Neither(idx_t begin, idx_t end) {
		}

		inline void Left(idx_t begin, idx_t end) {
			for (; begin < end; ++begin) {
				if (included(begin)) {
					state.ModeRm(begin);
				}
			}
		}

		inline void Right(idx_t begin, idx_t end) {
			for (; begin < end; ++begin) {
				if (included(begin)) {
					state.ModeAdd(begin);
				}
			}
		}

		inline void Both(idx_t begin, idx_t end) {
		}
	};

	template <class STATE, class INPUT_TYPE, class RESULT_TYPE>
	static void Window(AggregateInputData &aggr_input_data, const WindowPartitionInput &partition,
	                   const_data_ptr_t g_state, data_ptr_t l_state, const SubFrames &frames, Vector &result,
	                   idx_t rid) {
		auto &state = *reinterpret_cast<STATE *>(l_state);

		state.InitializePage(partition);
		const auto &fmask = partition.filter_mask;

		auto rdata = FlatVector::GetData<RESULT_TYPE>(result);
		auto &rmask = FlatVector::Validity(result);
		auto &prevs = state.prevs;
		if (prevs.empty()) {
			prevs.resize(1);
		}

		ModeIncluded<STATE> included(fmask, state);

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
						state.ModeAdd(i);
					}
				}
			}
		} else {
			using Updater = UpdateWindowState<STATE, INPUT_TYPE>;
			Updater updater(state, included);
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
};

template <typename TYPE_OP>
struct ModeFallbackFunction : BaseModeFunction<TYPE_OP> {
	template <class STATE>
	static void Finalize(STATE &state, AggregateFinalizeData &finalize_data) {
		if (!state.frequency_map) {
			finalize_data.ReturnNull();
			return;
		}
		auto highest_frequency = state.Scan();
		if (highest_frequency != state.frequency_map->end()) {
			CreateSortKeyHelpers::DecodeSortKey(highest_frequency->first, finalize_data.result,
			                                    finalize_data.result_idx,
			                                    OrderModifiers(OrderType::ASCENDING, OrderByNullType::NULLS_LAST));
		} else {
			finalize_data.ReturnNull();
		}
	}
};

AggregateFunction GetFallbackModeFunction(const LogicalType &type) {
	using STATE = ModeState<string_t, ModeString>;
	using OP = ModeFallbackFunction<ModeString>;
	AggregateFunction aggr({type}, type, AggregateFunction::StateSize<STATE>,
	                       AggregateFunction::StateInitialize<STATE, OP, AggregateDestructorType::LEGACY>,
	                       AggregateSortKeyHelpers::UnaryUpdate<STATE, OP>, AggregateFunction::StateCombine<STATE, OP>,
	                       AggregateFunction::StateVoidFinalize<STATE, OP>, nullptr);
	aggr.destructor = AggregateFunction::StateDestroy<STATE, OP>;
	return aggr;
}

template <typename INPUT_TYPE, typename TYPE_OP = ModeStandard<INPUT_TYPE>>
AggregateFunction GetTypedModeFunction(const LogicalType &type) {
	using STATE = ModeState<INPUT_TYPE, TYPE_OP>;
	using OP = ModeFunction<TYPE_OP>;
	auto func =
	    AggregateFunction::UnaryAggregateDestructor<STATE, INPUT_TYPE, INPUT_TYPE, OP, AggregateDestructorType::LEGACY>(
	        type, type);
	func.window = OP::template Window<STATE, INPUT_TYPE, INPUT_TYPE>;
	return func;
}

AggregateFunction GetModeAggregate(const LogicalType &type) {
	switch (type.InternalType()) {
#ifndef DUCKDB_SMALLER_BINARY
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
		return GetTypedModeFunction<string_t, ModeString>(type);
#endif
	default:
		return GetFallbackModeFunction(type);
	}
}

unique_ptr<FunctionData> BindModeAggregate(ClientContext &context, AggregateFunction &function,
                                           vector<unique_ptr<Expression>> &arguments) {
	function = GetModeAggregate(arguments[0]->return_type);
	function.name = "mode";
	return nullptr;
}

AggregateFunctionSet ModeFun::GetFunctions() {
	AggregateFunctionSet mode("mode");
	mode.AddFunction(AggregateFunction({LogicalTypeId::ANY}, LogicalTypeId::ANY, nullptr, nullptr, nullptr, nullptr,
	                                   nullptr, nullptr, BindModeAggregate));
	return mode;
}

//===--------------------------------------------------------------------===//
// Entropy
//===--------------------------------------------------------------------===//
template <class STATE>
static double FinalizeEntropy(STATE &state) {
	if (!state.frequency_map) {
		return 0;
	}
	double count = static_cast<double>(state.count);
	double entropy = 0;
	for (auto &val : *state.frequency_map) {
		double val_sec = static_cast<double>(val.second.count);
		entropy += (val_sec / count) * log2(count / val_sec);
	}
	return entropy;
}

template <typename TYPE_OP>
struct EntropyFunction : TypedModeFunction<TYPE_OP> {
	template <class T, class STATE>
	static void Finalize(STATE &state, T &target, AggregateFinalizeData &finalize_data) {
		target = FinalizeEntropy(state);
	}
};

template <typename TYPE_OP>
struct EntropyFallbackFunction : BaseModeFunction<TYPE_OP> {
	template <class T, class STATE>
	static void Finalize(STATE &state, T &target, AggregateFinalizeData &finalize_data) {
		target = FinalizeEntropy(state);
	}
};

template <typename INPUT_TYPE, typename TYPE_OP = ModeStandard<INPUT_TYPE>>
AggregateFunction GetTypedEntropyFunction(const LogicalType &type) {
	using STATE = ModeState<INPUT_TYPE, TYPE_OP>;
	using OP = EntropyFunction<TYPE_OP>;
	auto func =
	    AggregateFunction::UnaryAggregateDestructor<STATE, INPUT_TYPE, double, OP, AggregateDestructorType::LEGACY>(
	        type, LogicalType::DOUBLE);
	func.null_handling = FunctionNullHandling::SPECIAL_HANDLING;
	return func;
}

AggregateFunction GetFallbackEntropyFunction(const LogicalType &type) {
	using STATE = ModeState<string_t, ModeString>;
	using OP = EntropyFallbackFunction<ModeString>;
	AggregateFunction func({type}, LogicalType::DOUBLE, AggregateFunction::StateSize<STATE>,
	                       AggregateFunction::StateInitialize<STATE, OP, AggregateDestructorType::LEGACY>,
	                       AggregateSortKeyHelpers::UnaryUpdate<STATE, OP>, AggregateFunction::StateCombine<STATE, OP>,
	                       AggregateFunction::StateFinalize<STATE, double, OP>, nullptr);
	func.destructor = AggregateFunction::StateDestroy<STATE, OP>;
	func.null_handling = FunctionNullHandling::SPECIAL_HANDLING;
	return func;
}

AggregateFunction GetEntropyFunction(const LogicalType &type) {
	switch (type.InternalType()) {
#ifndef DUCKDB_SMALLER_BINARY
	case PhysicalType::UINT16:
		return GetTypedEntropyFunction<uint16_t>(type);
	case PhysicalType::UINT32:
		return GetTypedEntropyFunction<uint32_t>(type);
	case PhysicalType::UINT64:
		return GetTypedEntropyFunction<uint64_t>(type);
	case PhysicalType::INT16:
		return GetTypedEntropyFunction<int16_t>(type);
	case PhysicalType::INT32:
		return GetTypedEntropyFunction<int32_t>(type);
	case PhysicalType::INT64:
		return GetTypedEntropyFunction<int64_t>(type);
	case PhysicalType::FLOAT:
		return GetTypedEntropyFunction<float>(type);
	case PhysicalType::DOUBLE:
		return GetTypedEntropyFunction<double>(type);
	case PhysicalType::VARCHAR:
		return GetTypedEntropyFunction<string_t, ModeString>(type);
#endif
	default:
		return GetFallbackEntropyFunction(type);
	}
}

unique_ptr<FunctionData> BindEntropyAggregate(ClientContext &context, AggregateFunction &function,
                                              vector<unique_ptr<Expression>> &arguments) {
	function = GetEntropyFunction(arguments[0]->return_type);
	function.name = "entropy";
	return nullptr;
}

AggregateFunctionSet EntropyFun::GetFunctions() {
	AggregateFunctionSet entropy("entropy");
	entropy.AddFunction(AggregateFunction({LogicalTypeId::ANY}, LogicalType::DOUBLE, nullptr, nullptr, nullptr, nullptr,
	                                      nullptr, nullptr, BindEntropyAggregate));
	return entropy;
}

} // namespace duckdb
