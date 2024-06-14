#include "duckdb/function/scalar/nested_functions.hpp"
#include "duckdb/core_functions/aggregate/nested_functions.hpp"
#include "duckdb/planner/expression/bound_aggregate_expression.hpp"
#include "duckdb/common/pair.hpp"
#include "duckdb/planner/expression/bound_function_expression.hpp"
#include "duckdb/common/types/vector.hpp"
#include "duckdb/core_functions/create_sort_key.hpp"

namespace duckdb {

template <class T>
struct HistogramBinState {
	unsafe_vector<T> *bin_boundaries;
	unsafe_vector<idx_t> *counts;

	void Initialize() {
		bin_boundaries = nullptr;
		counts = nullptr;
	}

	void Destroy() {
		if (bin_boundaries) {
			delete bin_boundaries;
			bin_boundaries = nullptr;
		}
		if (counts) {
			delete counts;
			counts = nullptr;
		}
	}

	bool IsSet() {
		return bin_boundaries;
	}

	template<class OP>
	void InitializeBins(Vector &bin_vector, idx_t count, idx_t pos) {
		bin_boundaries = new unsafe_vector<T>();
		counts = new unsafe_vector<idx_t>();
		UnifiedVectorFormat bin_data;
		bin_vector.ToUnifiedFormat(count, bin_data);
		auto bin_counts = UnifiedVectorFormat::GetData<list_entry_t>(bin_data);
		auto bin_index = bin_data.sel->get_index(pos);
		auto bin_list = bin_counts[bin_index];
		if (!bin_data.validity.RowIsValid(bin_index)) {
			throw BinderException("Histogram bin list cannot be NULL");
		}
		if (bin_list.length == 0) {
			throw BinderException("Histogram bin list cannot be empty");
		}

		auto &bin_child = ListVector::GetEntry(bin_vector);
		UnifiedVectorFormat bin_child_data;
		auto extra_state = OP::CreateExtraState();
		OP::PrepareData(bin_child, ListVector::GetListSize(bin_vector), extra_state, bin_child_data);

		bin_boundaries->reserve(bin_list.length);
		for(idx_t i = 0; i < bin_list.length; i++) {
			auto bin_child_idx = bin_child_data.sel->get_index(bin_list.offset + i);
			if (!bin_child_data.validity.RowIsValid(bin_child_idx)) {
				throw BinderException("Histogram bin entry cannot be NULL");
			}
			OP::template ExtractBoundary<T>(*bin_boundaries, bin_child_data, bin_list.offset + i);
		}
		counts->resize(bin_list.length + 1);
	}
};

struct HistogramBinFunctor {
	template <class T>
	static void HistogramFinalize(T value, Vector &result, idx_t offset) {
		FlatVector::GetData<T>(result)[offset] = value;
	}

	static bool CreateExtraState() {
		return false;
	}

	static void PrepareData(Vector &input, idx_t count, bool &, UnifiedVectorFormat &result) {
		input.ToUnifiedFormat(count, result);
	}

	template <class T>
	static void ExtractBoundary(unsafe_vector<T> &boundaries, UnifiedVectorFormat &bin_data, idx_t offset) {
		boundaries.push_back(UnifiedVectorFormat::GetData<T>(bin_data)[bin_data.sel->get_index(offset)]);
	}
};

struct HistogramBinStringFunctorBase {
	template <class T>
	static void HistogramFinalize(T value, Vector &result, idx_t offset) {
		FlatVector::GetData<string_t>(result)[offset] = StringVector::AddStringOrBlob(result, value);
	}

	template <class T>
	static void ExtractBoundary(unsafe_vector<T> &boundaries, UnifiedVectorFormat &bin_data, idx_t offset) {
		throw InternalException("FIXME extract string boundary");
	}
};

struct HistogramBinStringFunctor : HistogramBinStringFunctorBase {
	static bool CreateExtraState() {
		return false;
	}

	static void PrepareData(Vector &input, idx_t count, bool &, UnifiedVectorFormat &result) {
		input.ToUnifiedFormat(count, result);
	}

	template <class T>
	static void ExtractBoundary(unsafe_vector<T> &boundaries, UnifiedVectorFormat &bin_data, idx_t offset) {
		throw InternalException("FIXME extract generic");
	}
};

struct HistogramBinGenericFunctor : HistogramBinStringFunctorBase {
	template <class T>
	static void HistogramFinalize(T value, Vector &result, idx_t offset) {
		CreateSortKeyHelpers::DecodeSortKey(value, result, offset,
		                                    OrderModifiers(OrderType::ASCENDING, OrderByNullType::NULLS_LAST));
	}

	static Vector CreateExtraState() {
		return Vector(LogicalType::BLOB);
	}

	static void PrepareData(Vector &input, idx_t count, Vector &extra_state, UnifiedVectorFormat &result) {
		OrderModifiers modifiers(OrderType::ASCENDING, OrderByNullType::NULLS_LAST);
		CreateSortKeyHelpers::CreateSortKey(input, count, modifiers, extra_state);
		input.Flatten(count);
		extra_state.Flatten(count);
		FlatVector::Validity(extra_state).Initialize(FlatVector::Validity(input));
		extra_state.ToUnifiedFormat(count, result);
	}
};

struct HistogramBinFunction {
	template <class STATE>
	static void Initialize(STATE &state) {
		state.Initialize();
	}

	template <class STATE>
	static void Destroy(STATE &state, AggregateInputData &aggr_input_data) {
		state.Destroy();
	}

	static bool IgnoreNull() {
		return true;
	}
};

template <class OP, class T>
static void HistogramBinUpdateFunction(Vector inputs[], AggregateInputData &, idx_t input_count, Vector &state_vector,
                                    idx_t count) {
	auto &input = inputs[0];
	UnifiedVectorFormat sdata;
	state_vector.ToUnifiedFormat(count, sdata);

	auto &bin_vector = inputs[1];

	auto extra_state = OP::CreateExtraState();
	UnifiedVectorFormat input_data;
	OP::PrepareData(input, count, extra_state, input_data);

	auto states = UnifiedVectorFormat::GetData<HistogramBinState<T> *>(sdata);
	auto data = UnifiedVectorFormat::GetData<T>(input_data);
	for (idx_t i = 0; i < count; i++) {
		auto idx = input_data.sel->get_index(i);
		if (!input_data.validity.RowIsValid(idx)) {
			continue;
		}
		auto &state = *states[sdata.sel->get_index(i)];
		if (!state.IsSet()) {
			state.template InitializeBins<OP>(bin_vector, count, i);
		}
		auto entry = std::lower_bound(state.bin_boundaries->begin(), state.bin_boundaries->end(), data[idx]);
		auto bin_entry = UnsafeNumericCast<idx_t>(entry - state.bin_boundaries->begin());
		++(*state.counts)[bin_entry];
	}
}

template <class T>
static void HistogramBinCombineFunction(Vector &state_vector, Vector &combined, AggregateInputData &, idx_t count) {

	UnifiedVectorFormat sdata;
	state_vector.ToUnifiedFormat(count, sdata);
	auto states_ptr = UnifiedVectorFormat::GetData<HistogramBinState<T> *>(sdata);

	auto combined_ptr = FlatVector::GetData<HistogramBinState<T> *>(combined);
	for (idx_t i = 0; i < count; i++) {
		auto &state = *states_ptr[sdata.sel->get_index(i)];
		if (!state.bin_boundaries) {
			// nothing to combine
			continue;
		}
		auto &target = *combined_ptr[i];
		if (!target.bin_boundaries) {
			// target does not have bin boundaries - copy everything over
			target.bin_boundaries = new unsafe_vector<T>();
			target.counts = new unsafe_vector<idx_t>();
			*target.bin_boundaries = *state.bin_boundaries;
			*target.counts = *state.counts;
		} else {
			// both source and target have bin boundaries
			if (*target.bin_boundaries != *state.bin_boundaries) {
				throw NotImplementedException("Histogram - cannot combine histograms with different bin boundaries. Bin boundaries must be the same for all histograms");
			}
			if (target.counts->size() != state.counts->size()) {
				throw InternalException("Histogram combine - bin boundaries are the same but counts are different");
			}
			D_ASSERT(target.counts->size() == state.counts->size());
			for(idx_t bin_idx = 0; bin_idx < target.counts->size(); bin_idx++) {
				(*target.counts)[bin_idx] += (*state.counts)[bin_idx];
			}
		}
	}
}

template <class OP, class T>
static void HistogramBinFinalizeFunction(Vector &state_vector, AggregateInputData &, Vector &result, idx_t count,
                                      idx_t offset) {
	UnifiedVectorFormat sdata;
	state_vector.ToUnifiedFormat(count, sdata);
	auto states = UnifiedVectorFormat::GetData<HistogramBinState<T> *>(sdata);

	auto &mask = FlatVector::Validity(result);
	auto old_len = ListVector::GetListSize(result);
	idx_t new_entries = 0;
	// figure out how much space we need
	for (idx_t i = 0; i < count; i++) {
		auto &state = *states[sdata.sel->get_index(i)];
		if (!state.bin_boundaries) {
			continue;
		}
		new_entries += state.bin_boundaries->size();
	}
	// reserve space in the list vector
	ListVector::Reserve(result, old_len + new_entries);
	auto &keys = MapVector::GetKeys(result);
	auto &values = MapVector::GetValues(result);
	auto list_entries = FlatVector::GetData<list_entry_t>(result);
	auto count_entries = FlatVector::GetData<uint64_t>(values);

	idx_t current_offset = old_len;
	for (idx_t i = 0; i < count; i++) {
		const auto rid = i + offset;
		auto &state = *states[sdata.sel->get_index(i)];
		if (!state.bin_boundaries) {
			mask.SetInvalid(rid);
			continue;
		}

		auto &list_entry = list_entries[rid];
		list_entry.offset = current_offset;
		for(idx_t bin_idx = 0; bin_idx < state.bin_boundaries->size(); bin_idx++) {
			OP::template HistogramFinalize<T>((*state.bin_boundaries)[bin_idx], keys, current_offset);
			count_entries[current_offset] = (*state.counts)[bin_idx];
			current_offset++;
		}
		list_entry.length = current_offset - list_entry.offset;
	}
	D_ASSERT(current_offset == old_len + new_entries);
	ListVector::SetListSize(result, current_offset);
	result.Verify(count);
}

template <class OP, class T>
static AggregateFunction GetHistogramBinFunction(const LogicalType &type) {
	using STATE_TYPE = HistogramBinState<T>;

	auto struct_type = LogicalType::MAP(type, LogicalType::UBIGINT);
	return AggregateFunction("histogram", {type, LogicalType::LIST(type)}, struct_type, AggregateFunction::StateSize<STATE_TYPE>,
	                         AggregateFunction::StateInitialize<STATE_TYPE, HistogramBinFunction>,
	                         HistogramBinUpdateFunction<OP, T>, HistogramBinCombineFunction<T>,
	                         HistogramBinFinalizeFunction<OP, T>, nullptr, nullptr,
	                         AggregateFunction::StateDestroy<STATE_TYPE, HistogramBinFunction>);
}

AggregateFunction GetHistogramBinFunction(const LogicalType &type) {
	switch (type.InternalType()) {
	case PhysicalType::BOOL:
		return GetHistogramBinFunction<HistogramBinFunctor, bool>(type);
	case PhysicalType::UINT8:
		return GetHistogramBinFunction<HistogramBinFunctor, uint8_t>(type);
	case PhysicalType::UINT16:
		return GetHistogramBinFunction<HistogramBinFunctor, uint16_t>(type);
	case PhysicalType::UINT32:
		return GetHistogramBinFunction<HistogramBinFunctor, uint32_t>(type);
	case PhysicalType::UINT64:
		return GetHistogramBinFunction<HistogramBinFunctor, uint64_t>(type);
	case PhysicalType::INT8:
		return GetHistogramBinFunction<HistogramBinFunctor, int8_t>(type);
	case PhysicalType::INT16:
		return GetHistogramBinFunction<HistogramBinFunctor, int16_t>(type);
	case PhysicalType::INT32:
		return GetHistogramBinFunction<HistogramBinFunctor, int32_t>(type);
	case PhysicalType::INT64:
		return GetHistogramBinFunction<HistogramBinFunctor, int64_t>(type);
	case PhysicalType::FLOAT:
		return GetHistogramBinFunction<HistogramBinFunctor, float>(type);
	case PhysicalType::DOUBLE:
		return GetHistogramBinFunction<HistogramBinFunctor, double>(type);
	default:
		throw InternalException("FIXME bin function type");
	// case PhysicalType::VARCHAR:
	// 	return GetHistogramBinFunction<HistogramBinStringFunctor, string_t>(type);
	// default:
	// 	return GetHistogramBinFunction<HistogramBinGenericFunctor, string_t>(type);
	}
}

unique_ptr<FunctionData> HistogramBinBindFunction(ClientContext &context, AggregateFunction &function,
                                               vector<unique_ptr<Expression>> &arguments) {
	for(auto &arg : arguments) {
		if (arg->return_type.id() == LogicalTypeId::UNKNOWN) {
			throw ParameterNotResolvedException();
		}
	}

	function = GetHistogramBinFunction(arguments[0]->return_type);
	return nullptr;
}

AggregateFunction HistogramFun::BinnedHistogramFunction() {
	return AggregateFunction("histogram", {LogicalType::ANY, LogicalType::LIST(LogicalType::ANY)}, LogicalTypeId::MAP, nullptr, nullptr, nullptr,
	                                     nullptr, nullptr, nullptr, HistogramBinBindFunction, nullptr);
}

} // namespace duckdb
