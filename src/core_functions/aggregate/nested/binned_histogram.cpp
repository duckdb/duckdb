#include "duckdb/function/scalar/nested_functions.hpp"
#include "duckdb/core_functions/aggregate/nested_functions.hpp"
#include "duckdb/planner/expression/bound_aggregate_expression.hpp"
#include "duckdb/common/types/vector.hpp"
#include "duckdb/core_functions/aggregate/histogram_helpers.hpp"
#include "duckdb/core_functions/scalar/generic_functions.hpp"
#include "duckdb/common/vector_operations/vector_operations.hpp"
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
		auto bin_count = ListVector::GetListSize(bin_vector);
		UnifiedVectorFormat bin_child_data;
		auto extra_state = OP::CreateExtraState(bin_count);
		OP::PrepareData(bin_child, bin_count, extra_state, bin_child_data);

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

struct HistogramRange {
	static constexpr bool EXACT = false;

	template <class T>
	static idx_t GetBin(T value, const unsafe_vector<T> &bin_boundaries) {
		auto entry = std::lower_bound(bin_boundaries.begin(), bin_boundaries.end(), value);
		return UnsafeNumericCast<idx_t>(entry - bin_boundaries.begin());
	}
};

struct HistogramExact {
	static constexpr bool EXACT = true;

	template <class T>
	static idx_t GetBin(T value, const unsafe_vector<T> &bin_boundaries) {
		auto entry = std::lower_bound(bin_boundaries.begin(), bin_boundaries.end(), value);
		if (entry == bin_boundaries.end() || !(*entry == value)) {
			// entry not found - return last bucket
			return bin_boundaries.size();
		}
		return UnsafeNumericCast<idx_t>(entry - bin_boundaries.begin());
	}
};

template <class OP, class T, class HIST>
static void HistogramBinUpdateFunction(Vector inputs[], AggregateInputData &aggr_input, idx_t input_count,
                                       Vector &state_vector, idx_t count) {
	auto &input = inputs[0];
	UnifiedVectorFormat sdata;
	state_vector.ToUnifiedFormat(count, sdata);

	auto &bin_vector = inputs[1];

	auto extra_state = OP::CreateExtraState(count);
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
		auto bin_entry = HIST::template GetBin<T>(data[idx], *state.bin_boundaries);
		++(*state.counts)[bin_entry];
	}
}

static bool SupportsOtherBucket(const LogicalType &type) {
	if (type.HasAlias()) {
		return false;
	}
	switch (type.id()) {
	case LogicalTypeId::TINYINT:
	case LogicalTypeId::SMALLINT:
	case LogicalTypeId::INTEGER:
	case LogicalTypeId::BIGINT:
	case LogicalTypeId::HUGEINT:
	case LogicalTypeId::FLOAT:
	case LogicalTypeId::DOUBLE:
	case LogicalTypeId::DECIMAL:
	case LogicalTypeId::UTINYINT:
	case LogicalTypeId::USMALLINT:
	case LogicalTypeId::UINTEGER:
	case LogicalTypeId::UBIGINT:
	case LogicalTypeId::UHUGEINT:
	case LogicalTypeId::TIME:
	case LogicalTypeId::TIME_TZ:
	case LogicalTypeId::DATE:
	case LogicalTypeId::TIMESTAMP:
	case LogicalTypeId::TIMESTAMP_TZ:
	case LogicalTypeId::TIMESTAMP_SEC:
	case LogicalTypeId::TIMESTAMP_MS:
	case LogicalTypeId::TIMESTAMP_NS:
	case LogicalTypeId::VARCHAR:
	case LogicalTypeId::BLOB:
	case LogicalTypeId::STRUCT:
	case LogicalTypeId::LIST:
		return true;
	default:
		return false;
	}
}
static Value OtherBucketValue(const LogicalType &type) {
	switch (type.id()) {
	case LogicalTypeId::TINYINT:
	case LogicalTypeId::SMALLINT:
	case LogicalTypeId::INTEGER:
	case LogicalTypeId::BIGINT:
	case LogicalTypeId::HUGEINT:
	case LogicalTypeId::DECIMAL:
	case LogicalTypeId::UTINYINT:
	case LogicalTypeId::USMALLINT:
	case LogicalTypeId::UINTEGER:
	case LogicalTypeId::UBIGINT:
	case LogicalTypeId::UHUGEINT:
	case LogicalTypeId::TIME:
	case LogicalTypeId::TIME_TZ:
		return Value::MaximumValue(type);
	case LogicalTypeId::DATE:
	case LogicalTypeId::TIMESTAMP:
	case LogicalTypeId::TIMESTAMP_TZ:
	case LogicalTypeId::TIMESTAMP_SEC:
	case LogicalTypeId::TIMESTAMP_MS:
	case LogicalTypeId::TIMESTAMP_NS:
	case LogicalTypeId::FLOAT:
	case LogicalTypeId::DOUBLE:
		return Value::Infinity(type);
	case LogicalTypeId::VARCHAR:
		return Value("");
	case LogicalTypeId::BLOB:
		return Value::BLOB("");
	case LogicalTypeId::STRUCT: {
		// for structs we can set all child members to NULL
		auto &child_types = StructType::GetChildTypes(type);
		child_list_t<Value> child_list;
		for (auto &child_type : child_types) {
			child_list.push_back(make_pair(child_type.first, Value(child_type.second)));
		}
		return Value::STRUCT(std::move(child_list));
	}
	case LogicalTypeId::LIST:
		return Value::EMPTYLIST(ListType::GetChildType(type));
	default:
		throw InternalException("Unsupported type for other bucket");
	}
}

static void IsHistogramOtherBinFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	auto &input_type = args.data[0].GetType();
	if (!SupportsOtherBucket(input_type)) {
		result.Reference(Value::BOOLEAN(false));
		return;
	}
	auto v = OtherBucketValue(input_type);
	Vector ref(v);
	VectorOperations::NotDistinctFrom(args.data[0], ref, result, args.size());
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
	bool supports_other_bucket = SupportsOtherBucket(MapType::KeyType(result.GetType()));
	// figure out how much space we need
	for (idx_t i = 0; i < count; i++) {
		auto &state = *states[sdata.sel->get_index(i)];
		if (!state.bin_boundaries) {
			continue;
		}
		new_entries += state.bin_boundaries->size();
		if (state.counts->back() > 0 && supports_other_bucket) {
			// overflow bucket has entries
			new_entries++;
		}
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
		if (state.counts->back() > 0 && supports_other_bucket) {
			// add overflow bucket ("others")
			// set bin boundary to NULL for overflow bucket
			keys.SetValue(current_offset, OtherBucketValue(keys.GetType()));
			count_entries[current_offset] = state.counts->back();
			current_offset++;
		}
		list_entry.length = current_offset - list_entry.offset;
	}
	D_ASSERT(current_offset == old_len + new_entries);
	ListVector::SetListSize(result, current_offset);
	result.Verify(count);
}

template <class OP, class T, class HIST>
static AggregateFunction GetHistogramBinFunction(const LogicalType &type) {
	using STATE_TYPE = HistogramBinState<T>;

	const char *function_name = HIST::EXACT ? "histogram_exact" : "histogram";

	auto struct_type = LogicalType::MAP(type, LogicalType::UBIGINT);
	return AggregateFunction(
	    function_name, {type, LogicalType::LIST(type)}, struct_type, AggregateFunction::StateSize<STATE_TYPE>,
	    AggregateFunction::StateInitialize<STATE_TYPE, HistogramBinFunction>, HistogramBinUpdateFunction<OP, T, HIST>,
	    AggregateFunction::StateCombine<STATE_TYPE, HistogramBinFunction>, HistogramBinFinalizeFunction<OP, T>, nullptr,
	    nullptr, AggregateFunction::StateDestroy<STATE_TYPE, HistogramBinFunction>);
}

template <class HIST>
AggregateFunction GetHistogramBinFunction(const LogicalType &type) {
	switch (type.InternalType()) {
	case PhysicalType::BOOL:
		return GetHistogramBinFunction<HistogramFunctor, bool, HIST>(type);
	case PhysicalType::UINT8:
		return GetHistogramBinFunction<HistogramFunctor, uint8_t, HIST>(type);
	case PhysicalType::UINT16:
		return GetHistogramBinFunction<HistogramFunctor, uint16_t, HIST>(type);
	case PhysicalType::UINT32:
		return GetHistogramBinFunction<HistogramFunctor, uint32_t, HIST>(type);
	case PhysicalType::UINT64:
		return GetHistogramBinFunction<HistogramFunctor, uint64_t, HIST>(type);
	case PhysicalType::INT8:
		return GetHistogramBinFunction<HistogramFunctor, int8_t, HIST>(type);
	case PhysicalType::INT16:
		return GetHistogramBinFunction<HistogramFunctor, int16_t, HIST>(type);
	case PhysicalType::INT32:
		return GetHistogramBinFunction<HistogramFunctor, int32_t, HIST>(type);
	case PhysicalType::INT64:
		return GetHistogramBinFunction<HistogramFunctor, int64_t, HIST>(type);
	case PhysicalType::FLOAT:
		return GetHistogramBinFunction<HistogramFunctor, float, HIST>(type);
	case PhysicalType::DOUBLE:
		return GetHistogramBinFunction<HistogramFunctor, double, HIST>(type);
	case PhysicalType::VARCHAR:
		return GetHistogramBinFunction<HistogramStringFunctor, string_t, HIST>(type);
	default:
		return GetHistogramBinFunction<HistogramGenericFunctor, string_t, HIST>(type);
	}
}

template <class HIST>
unique_ptr<FunctionData> HistogramBinBindFunction(ClientContext &context, AggregateFunction &function,
                                                  vector<unique_ptr<Expression>> &arguments) {
	for (auto &arg : arguments) {
		if (arg->return_type.id() == LogicalTypeId::UNKNOWN) {
			throw ParameterNotResolvedException();
		}
	}

	function = GetHistogramBinFunction<HIST>(arguments[0]->return_type);
	return nullptr;
}

AggregateFunction HistogramFun::BinnedHistogramFunction() {
	return AggregateFunction("histogram", {LogicalType::ANY, LogicalType::LIST(LogicalType::ANY)}, LogicalTypeId::MAP,
	                         nullptr, nullptr, nullptr, nullptr, nullptr, nullptr,
	                         HistogramBinBindFunction<HistogramRange>, nullptr);
}

AggregateFunction HistogramExactFun::GetFunction() {
	return AggregateFunction("histogram_exact", {LogicalType::ANY, LogicalType::LIST(LogicalType::ANY)},
	                         LogicalTypeId::MAP, nullptr, nullptr, nullptr, nullptr, nullptr, nullptr,
	                         HistogramBinBindFunction<HistogramExact>, nullptr);
}

ScalarFunction IsHistogramOtherBinFun::GetFunction() {
	return ScalarFunction("is_histogram_other_bin", {LogicalType::ANY}, LogicalType::BOOLEAN,
	                      IsHistogramOtherBinFunction);
}

} // namespace duckdb
