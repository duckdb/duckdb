#include "duckdb/function/scalar/nested_functions.hpp"
#include "duckdb/core_functions/aggregate/nested_functions.hpp"
#include "duckdb/planner/expression/bound_aggregate_expression.hpp"
#include "duckdb/common/types/vector.hpp"
#include "duckdb/core_functions/aggregate/histogram_helpers.hpp"
#include "duckdb/common/algorithm.hpp"

namespace duckdb {

template <class T>
struct HistogramBinState {
	using TYPE = T;

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

	template <class OP>
	void InitializeBins(Vector &bin_vector, idx_t count, idx_t pos, AggregateInputData &aggr_input) {
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

		auto &bin_child = ListVector::GetEntry(bin_vector);
		UnifiedVectorFormat bin_child_data;
		auto extra_state = OP::CreateExtraState();
		OP::PrepareData(bin_child, ListVector::GetListSize(bin_vector), extra_state, bin_child_data);

		bin_boundaries->reserve(bin_list.length);
		for (idx_t i = 0; i < bin_list.length; i++) {
			auto bin_child_idx = bin_child_data.sel->get_index(bin_list.offset + i);
			if (!bin_child_data.validity.RowIsValid(bin_child_idx)) {
				throw BinderException("Histogram bin entry cannot be NULL");
			}
			bin_boundaries->push_back(OP::template ExtractValue<T>(bin_child_data, bin_list.offset + i, aggr_input));
		}
		// sort the bin boundaries
		std::sort(bin_boundaries->begin(), bin_boundaries->end());
		// ensure there are no duplicate bin boundaries
		for (idx_t i = 1; i < bin_boundaries->size(); i++) {
			if (Equals::Operation((*bin_boundaries)[i - 1], (*bin_boundaries)[i])) {
				bin_boundaries->erase_at(i);
				i--;
			}
		}

		counts->resize(bin_list.length + 1);
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

	template <class STATE, class OP>
	static void Combine(const STATE &source, STATE &target, AggregateInputData &input_data) {
		if (!source.bin_boundaries) {
			// nothing to combine
			return;
		}
		if (!target.bin_boundaries) {
			// target does not have bin boundaries - copy everything over
			target.bin_boundaries = new unsafe_vector<typename STATE::TYPE>();
			target.counts = new unsafe_vector<idx_t>();
			*target.bin_boundaries = *source.bin_boundaries;
			*target.counts = *source.counts;
		} else {
			// both source and target have bin boundaries
			if (*target.bin_boundaries != *source.bin_boundaries) {
				throw NotImplementedException(
				    "Histogram - cannot combine histograms with different bin boundaries. "
				    "Bin boundaries must be the same for all histograms within the same group");
			}
			if (target.counts->size() != source.counts->size()) {
				throw InternalException("Histogram combine - bin boundaries are the same but counts are different");
			}
			D_ASSERT(target.counts->size() == source.counts->size());
			for (idx_t bin_idx = 0; bin_idx < target.counts->size(); bin_idx++) {
				(*target.counts)[bin_idx] += (*source.counts)[bin_idx];
			}
		}
	}
};

template <class OP, class T>
static void HistogramBinUpdateFunction(Vector inputs[], AggregateInputData &aggr_input, idx_t input_count,
                                       Vector &state_vector, idx_t count) {
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
			state.template InitializeBins<OP>(bin_vector, count, i, aggr_input);
		}
		auto entry = std::lower_bound(state.bin_boundaries->begin(), state.bin_boundaries->end(), data[idx]);
		auto bin_entry = UnsafeNumericCast<idx_t>(entry - state.bin_boundaries->begin());
		++(*state.counts)[bin_entry];
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
		for (idx_t bin_idx = 0; bin_idx < state.bin_boundaries->size(); bin_idx++) {
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
	return AggregateFunction(
	    "histogram", {type, LogicalType::LIST(type)}, struct_type, AggregateFunction::StateSize<STATE_TYPE>,
	    AggregateFunction::StateInitialize<STATE_TYPE, HistogramBinFunction>, HistogramBinUpdateFunction<OP, T>,
	    AggregateFunction::StateCombine<STATE_TYPE, HistogramBinFunction>, HistogramBinFinalizeFunction<OP, T>, nullptr,
	    nullptr, AggregateFunction::StateDestroy<STATE_TYPE, HistogramBinFunction>);
}

AggregateFunction GetHistogramBinFunction(const LogicalType &type) {
	switch (type.InternalType()) {
	case PhysicalType::BOOL:
		return GetHistogramBinFunction<HistogramFunctor, bool>(type);
	case PhysicalType::UINT8:
		return GetHistogramBinFunction<HistogramFunctor, uint8_t>(type);
	case PhysicalType::UINT16:
		return GetHistogramBinFunction<HistogramFunctor, uint16_t>(type);
	case PhysicalType::UINT32:
		return GetHistogramBinFunction<HistogramFunctor, uint32_t>(type);
	case PhysicalType::UINT64:
		return GetHistogramBinFunction<HistogramFunctor, uint64_t>(type);
	case PhysicalType::INT8:
		return GetHistogramBinFunction<HistogramFunctor, int8_t>(type);
	case PhysicalType::INT16:
		return GetHistogramBinFunction<HistogramFunctor, int16_t>(type);
	case PhysicalType::INT32:
		return GetHistogramBinFunction<HistogramFunctor, int32_t>(type);
	case PhysicalType::INT64:
		return GetHistogramBinFunction<HistogramFunctor, int64_t>(type);
	case PhysicalType::FLOAT:
		return GetHistogramBinFunction<HistogramFunctor, float>(type);
	case PhysicalType::DOUBLE:
		return GetHistogramBinFunction<HistogramFunctor, double>(type);
	case PhysicalType::VARCHAR:
		return GetHistogramBinFunction<HistogramStringFunctor, string_t>(type);
	default:
		return GetHistogramBinFunction<HistogramGenericFunctor, string_t>(type);
	}
}

unique_ptr<FunctionData> HistogramBinBindFunction(ClientContext &context, AggregateFunction &function,
                                                  vector<unique_ptr<Expression>> &arguments) {
	for (auto &arg : arguments) {
		if (arg->return_type.id() == LogicalTypeId::UNKNOWN) {
			throw ParameterNotResolvedException();
		}
	}

	function = GetHistogramBinFunction(arguments[0]->return_type);
	return nullptr;
}

AggregateFunction HistogramFun::BinnedHistogramFunction() {
	return AggregateFunction("histogram", {LogicalType::ANY, LogicalType::LIST(LogicalType::ANY)}, LogicalTypeId::MAP,
	                         nullptr, nullptr, nullptr, nullptr, nullptr, nullptr, HistogramBinBindFunction, nullptr);
}

} // namespace duckdb
