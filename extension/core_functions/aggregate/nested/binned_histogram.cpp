#include "core_functions/aggregate/histogram_helpers.hpp"
#include "core_functions/aggregate/nested_functions.hpp"
#include "core_functions/scalar/generic_functions.hpp"
#include "duckdb/common/algorithm.hpp"
#include "duckdb/common/arena_containers/arena_vector.hpp"
#include "duckdb/common/smaller_binary.hpp"
#include "duckdb/common/types/sql_value_map.hpp"
#include "duckdb/common/types/vector.hpp"
#include "duckdb/common/vector/flat_vector.hpp"
#include "duckdb/common/vector/list_vector.hpp"
#include "duckdb/common/vector/map_vector.hpp"
#include "duckdb/common/vector_operations/vector_operations.hpp"
#include "duckdb/function/scalar/nested_functions.hpp"
#include "duckdb/planner/expression/bound_aggregate_expression.hpp"

namespace duckdb {

namespace {

//! All state memory is allocated from the aggregate's arena, so the state needs no destructor
template <class T>
struct HistogramBinState {
	using TYPE = T;

	unsafe_arena_vector<T> *bin_boundaries;
	unsafe_arena_vector<idx_t> *counts;

	bool IsSet() {
		return bin_boundaries;
	}

	template <class OP>
	void InitializeBins(Vector &bin_vector, idx_t pos, AggregateInputData &aggr_input) {
		auto &allocator = aggr_input.allocator;
		auto &new_boundaries = *allocator.Make<unsafe_arena_vector<T>>(allocator);
		auto &new_counts = *allocator.Make<unsafe_arena_vector<idx_t>>(allocator);
		auto bin_counts = bin_vector.Values<list_entry_t>();
		auto bin_entry = bin_counts[pos];
		if (!bin_entry.IsValid()) {
			throw BinderException("Histogram bin list cannot be NULL");
		}
		auto bin_list = bin_entry.GetValue();

		auto &bin_child = ListVector::GetChildMutable(bin_vector);
		UnifiedVectorFormat bin_child_data;
		auto extra_state = OP::CreateExtraState();
		OP::PrepareData(bin_child, extra_state, bin_child_data);

		new_boundaries.reserve(bin_list.length);
		for (idx_t i = 0; i < bin_list.length; i++) {
			auto bin_child_idx = bin_child_data.sel->get_index(bin_list.offset + i);
			if (!bin_child_data.validity.RowIsValid(bin_child_idx)) {
				throw BinderException("Histogram bin entry cannot be NULL");
			}
			new_boundaries.push_back(OP::template ExtractValue<T>(bin_child_data, bin_list.offset + i, aggr_input));
		}
		// sort the bin boundaries
		std::sort(new_boundaries.begin(), new_boundaries.end(), SQLValueLessThan<T>());
		// ensure there are no duplicate bin boundaries
		for (idx_t i = 1; i < new_boundaries.size(); i++) {
			if (Equals::Operation(new_boundaries[i - 1], new_boundaries[i])) {
				new_boundaries.erase_at(i);
				i--;
			}
		}

		new_counts.resize(new_boundaries.size() + 1);
		bin_boundaries = &new_boundaries;
		counts = &new_counts;
	}
};

struct HistogramBinFunction {
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
			// target does not have bin boundaries - copy everything over into the caller's arena
			// (element-wise: copy-assigning would adopt the source's arena)
			auto &allocator = input_data.allocator;
			auto &boundaries = *allocator.Make<unsafe_arena_vector<typename STATE::TYPE>>(allocator);
			auto &counts = *allocator.Make<unsafe_arena_vector<idx_t>>(allocator);
			boundaries.insert(boundaries.end(), source.bin_boundaries->begin(), source.bin_boundaries->end());
			counts.insert(counts.end(), source.counts->begin(), source.counts->end());
			target.bin_boundaries = &boundaries;
			target.counts = &counts;
		} else {
			// both source and target have bin boundaries
			auto &target_bins = *target.bin_boundaries;
			auto &source_bins = *source.bin_boundaries;
			if (target_bins.size() != source_bins.size() ||
			    !std::equal(target_bins.begin(), target_bins.end(), source_bins.begin(),
			                SQLValueEquality<typename STATE::TYPE>())) {
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
	static idx_t GetBin(T value, const unsafe_arena_vector<T> &bin_boundaries) {
		auto entry = std::lower_bound(bin_boundaries.begin(), bin_boundaries.end(), value, SQLValueLessThan<T>());
		return UnsafeNumericCast<idx_t>(entry - bin_boundaries.begin());
	}
};

struct HistogramExact {
	static constexpr bool EXACT = true;

	template <class T>
	static idx_t GetBin(T value, const unsafe_arena_vector<T> &bin_boundaries) {
		auto entry = std::lower_bound(bin_boundaries.begin(), bin_boundaries.end(), value, SQLValueLessThan<T>());
		if (entry == bin_boundaries.end() || !SQLValueEquality<T>()(*entry, value)) {
			// entry not found - return last bucket
			return bin_boundaries.size();
		}
		return UnsafeNumericCast<idx_t>(entry - bin_boundaries.begin());
	}
};

template <class OP, class T, class HIST>
void HistogramBinUpdateFunction(Vector inputs[], AggregateInputData &aggr_input, idx_t input_count,
                                Vector &state_vector, idx_t count) {
	auto &input = inputs[0];
	auto &bin_vector = inputs[1];

	auto extra_state = OP::CreateExtraState();
	UnifiedVectorFormat input_data;
	OP::PrepareData(input, extra_state, input_data);

	auto states = state_vector.Values<HistogramBinState<T> *>();
	auto data = UnifiedVectorFormat::GetData<T>(input_data);
	for (idx_t i = 0; i < count; i++) {
		auto idx = input_data.sel->get_index(i);
		if (!input_data.validity.RowIsValid(idx)) {
			continue;
		}
		auto &state = *states[i].GetValue();
		if (!state.IsSet()) {
			state.template InitializeBins<OP>(bin_vector, i, aggr_input);
		}
		auto bin_entry = HIST::template GetBin<T>(data[idx], *state.bin_boundaries);
		++(*state.counts)[bin_entry];
	}
}

bool SupportsOtherBucket(const LogicalType &type) {
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
	case LogicalTypeId::TIMESTAMP_TZ_NS:
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
Value OtherBucketValue(const LogicalType &type) {
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
	case LogicalTypeId::TIMESTAMP_TZ_NS:
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
		return Value::LIST(ListType::GetChildType(type), vector<Value>());
	default:
		throw InternalException("Unsupported type for other bucket");
	}
}

void IsHistogramOtherBinFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	auto &input_type = args.data[0].GetType();
	if (!SupportsOtherBucket(input_type)) {
		result.Reference(Value::BOOLEAN(false), count_t(args.size()));
		return;
	}
	auto v = OtherBucketValue(input_type);
	Vector ref(v, count_t(args.size()));
	VectorOperations::NotDistinctFrom(args.data[0], ref, result);

	// Set NULL if input is NULL.
	UnifiedVectorFormat input_data;
	args.data[0].ToUnifiedFormat(input_data);
	if (!input_data.validity.CannotHaveNull()) {
		auto &result_validity = FlatVector::ValidityMutable(result);
		for (idx_t idx = 0; idx < args.size(); ++idx) {
			auto input_idx = input_data.sel->get_index(idx);
			if (!input_data.validity.RowIsValid(input_idx)) {
				result_validity.SetInvalid(idx);
			}
		}
	}
}

template <class OP, class T>
void HistogramBinFinalizeFunction(Vector &state_vector, AggregateFinalizeInputData &, Vector &result, idx_t count,
                                  idx_t offset) {
	auto states = state_vector.Values<HistogramBinState<T> *>();

	auto &mask = FlatVector::ValidityMutable(result);
	auto old_len = ListVector::GetListSize(result);
	idx_t new_entries = 0;
	bool supports_other_bucket = SupportsOtherBucket(MapType::KeyType(result.GetType()));
	// figure out how much space we need
	for (idx_t i = 0; i < count; i++) {
		auto &state = *states[i].GetValue();
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
	auto list_entries = FlatVector::GetDataMutable<list_entry_t>(result);
	auto count_entries = FlatVector::GetDataMutable<uint64_t>(values);

	idx_t current_offset = old_len;
	for (idx_t i = 0; i < count; i++) {
		const auto rid = i + offset;
		auto &state = *states[i].GetValue();
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
			auto other_value = OtherBucketValue(keys.GetType());
			for (idx_t bin_idx = list_entry.offset; bin_idx < current_offset; bin_idx++) {
				if (Value::NotDistinctFrom(keys.GetValue(bin_idx), other_value)) {
					throw InvalidInputException(
					    "Histogram bin %s collides with the key of the \"other values\" bucket, "
					    "which has entries - use a different bin value",
					    other_value.ToSQLString());
				}
			}
			keys.SetValue(current_offset, other_value);
			count_entries[current_offset] = state.counts->back();
			current_offset++;
		}
		list_entry.length = current_offset - list_entry.offset;
	}
	D_ASSERT(current_offset == old_len + new_entries);
	ListVector::SetListSize(result, current_offset);
	result.Verify();
}

template <class OP, class T, class HIST>
AggregateFunction GetHistogramBinFunction(const LogicalType &type) {
	using STATE_TYPE = HistogramBinState<T>;

	const char *function_name = HIST::EXACT ? "histogram_exact" : "histogram";

	auto struct_type = LogicalType::MAP(type, LogicalType::UBIGINT);
	AggregateFunction function(function_name, {}, struct_type, AggregateFunction::StateSize<STATE_TYPE>,
	                           AggregateFunction::StateInitialize<STATE_TYPE, HistogramBinFunction>,
	                           HistogramBinUpdateFunction<OP, T, HIST>,
	                           AggregateFunction::StateCombine<STATE_TYPE, HistogramBinFunction>,
	                           HistogramBinFinalizeFunction<OP, T>, nullptr, nullptr, nullptr);
	function.GetSignature().AddParameter("arg", type).AddParameter("bins", LogicalType::LIST(type));
	// a NULL bin list or bin entry throws in the update, different bin boundaries of one group in the combine, and a
	// bin that is the key of the "other values" bucket while that bucket has entries in the finalize
	function.SetFallible();
	return function;
}

template <class HIST>
AggregateFunction GetHistogramBinFunction(const LogicalType &type) {
	if (type.id() == LogicalTypeId::DECIMAL) {
		return GetHistogramBinFunction<HIST>(LogicalType::DOUBLE);
	}
	switch (type.InternalType()) {
#if !DUCKDB_SMALLER_BINARY(binned_histogram_types)
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
#endif
	default:
		return GetHistogramBinFunction<HistogramGenericFunctor, string_t, HIST>(type);
	}
}

template <class HIST>
unique_ptr<FunctionData> HistogramBinBindFunction(BindAggregateFunctionInput &input) {
	auto &function = input.GetBoundFunction();
	auto &arguments = input.GetArguments();
	for (auto &arg : arguments) {
		if (arg->GetReturnType().id() == LogicalTypeId::UNKNOWN) {
			throw ParameterNotResolvedException();
		}
	}

	// the template signature resolved both parameters to the deduced bin type
	function.ReplaceImplementation(GetHistogramBinFunction<HIST>(function.GetArguments()[0]));
	return nullptr;
}

} // namespace

AggregateFunction HistogramFun::BinnedHistogramFunction() {
	AggregateFunction function("histogram", {}, LogicalTypeId::MAP, nullptr, nullptr, nullptr, nullptr, nullptr,
	                           nullptr, HistogramBinBindFunction<HistogramRange>, nullptr);
	function.GetSignature()
	    .AddParameter("arg", LogicalType::TEMPLATE("T"))
	    .AddParameter("bins", LogicalType::LIST(LogicalType::TEMPLATE("T")));
	return function;
}

AggregateFunction HistogramExactFun::GetFunction() {
	AggregateFunction function("histogram_exact", {}, LogicalTypeId::MAP, nullptr, nullptr, nullptr, nullptr, nullptr,
	                           nullptr, HistogramBinBindFunction<HistogramExact>, nullptr);
	function.GetSignature()
	    .AddParameter("arg", LogicalType::TEMPLATE("T"))
	    .AddParameter("bins", LogicalType::LIST(LogicalType::TEMPLATE("T")));
	return function;
}

ScalarFunction IsHistogramOtherBinFun::GetFunction() {
	ScalarFunction fun("is_histogram_other_bin", {}, LogicalType::BOOLEAN, IsHistogramOtherBinFunction);
	fun.GetSignature().AddParameter("val", LogicalType::ANY);
	return fun;
}

} // namespace duckdb
