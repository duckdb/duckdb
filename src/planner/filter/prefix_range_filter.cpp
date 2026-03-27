#include "duckdb/planner/filter/prefix_range_filter.hpp"

#include "duckdb/common/allocator.hpp"
#include "duckdb/common/assert.hpp"
#include "duckdb/common/bit_utils.hpp"
#include "duckdb/common/enums/filter_propagate_result.hpp"
#include "duckdb/common/enums/vector_type.hpp"
#include "duckdb/common/numeric_utils.hpp"
#include "duckdb/common/operator/subtract.hpp"
#include "duckdb/common/optional_ptr.hpp"
#include "duckdb/common/typedefs.hpp"
#include "duckdb/common/types.hpp"
#include "duckdb/common/types/selection_vector.hpp"
#include "duckdb/common/types/vector.hpp"
#include "duckdb/common/uhugeint.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/planner/expression/bound_constant_expression.hpp"
#include "duckdb/storage/statistics/string_stats.hpp"
#include "duckdb/storage/buffer_manager.hpp"
#include "duckdb/planner/table_filter_state.hpp"

namespace duckdb {

namespace {

AllocatedData AllocateBitmap(ClientContext &context, const idx_t word_count, uint64_t *&bitmap_begin) {
	const idx_t size = word_count * sizeof(uint64_t);
	BufferManager &buffer_manager = BufferManager::GetBufferManager(context);
	auto buffer = buffer_manager.GetBufferAllocator().Allocate(64ULL + size);
	bitmap_begin = reinterpret_cast<uint64_t *>((64ULL + reinterpret_cast<uint64_t>(buffer.get())) & ~63ULL);
	std::fill_n(bitmap_begin, word_count, 0);
	return buffer;
}

struct PrefixRangeBuildState : public PrefixRangeFilter::BuildState {
	explicit PrefixRangeBuildState(AllocatedData data_p, uint64_t *bitmap_p, idx_t word_count_p)
	    : data(std::move(data_p)), bitmap(bitmap_p), word_count(word_count_p) {
	}

	AllocatedData data;
	uint64_t *bitmap;
	idx_t word_count;
};

template <typename U>
class PrefixRangeBitmap {
public:
	void Initialize(ClientContext &context, U min_p, U max_p) {
		D_ASSERT(min_p <= max_p);
		min = min_p;
		span = max_p - min;
		shift = 0;

		if (span >= CAP_BITS) {
			const auto q = NumericCast<uint64_t>(span >> MAX_PREFIX_LENGTH);
			shift = (q <= 1) ? 0 : (64 - CountZeros<uint64_t>::Leading(q - 1));
		}

		const idx_t buckets = NumericCast<idx_t>((span >> shift) + 1);
		word_count = buckets == 0 ? 1 : (buckets + 63) >> WORD_SHIFT;

		buf_ = AllocateBitmap(context, word_count, bitmap);

		// Only mark initialized as true when local bitmaps are merged.
		initialized = false;
	}

	unique_ptr<PrefixRangeFilter::BuildState> InitializeBuildState(ClientContext &context) const {
		D_ASSERT(bitmap);
		uint64_t *state_bitmap;
		auto state_data = AllocateBitmap(context, word_count, state_bitmap);
		return make_uniq<PrefixRangeBuildState>(std::move(state_data), state_bitmap, word_count);
	}

	void Insert(U key, PrefixRangeFilter::BuildState &state_p) const {
		auto &state = static_cast<PrefixRangeBuildState &>(state_p);
		const U y = key - min;
		// All keys are in-range by construction, so the range check can be omitted here.
		const U idx = y >> shift;
		state.bitmap[idx >> WORD_SHIFT] |= 1ULL << (idx & WORD_MASK);
	}

	void MergeBuildState(PrefixRangeFilter::BuildState &state_p) {
		auto &state = static_cast<PrefixRangeBuildState &>(state_p);
		for (idx_t word_idx = 0; word_idx < word_count; word_idx++) {
			bitmap[word_idx] |= state.bitmap[word_idx];
		}
		initialized = true;
	}

	idx_t Lookup(U key) const {
		const U y = key - min;
		const U bit_idx = y >> shift;
		const uint8_t in_range = y <= span;
		const uint32_t word_idx = NumericCast<uint32_t>(bit_idx >> WORD_SHIFT) & (0U - in_range);
		const uint8_t bit = (bitmap[word_idx] >> (bit_idx & WORD_MASK)) & 1ULL;
		return bit & in_range;
	}

	FilterPropagateResult LookupRange(U lower_bound, U upper_bound) const {
		const U max = min + span;
		if (upper_bound < min || lower_bound > max) {
			return FilterPropagateResult::FILTER_ALWAYS_FALSE;
		}

		const auto adjusted_lb = MaxValue<U>(lower_bound, min);
		const auto adjusted_ub = MinValue<U>(upper_bound, max);

		const auto lb_bit_idx = (adjusted_lb - min) >> shift;
		const auto lb_word_idx = lb_bit_idx >> WORD_SHIFT;

		const auto ub_bit_idx = (adjusted_ub - min) >> shift;
		const auto ub_word_idx = ub_bit_idx >> WORD_SHIFT;

		const idx_t lb_bit_off = NumericCast<idx_t>(lb_bit_idx & NumericCast<U>(WORD_MASK));
		const idx_t ub_bit_off = NumericCast<idx_t>(ub_bit_idx & NumericCast<U>(WORD_MASK));

		// TODO: Count the amount of 1's in the range, compare to a threshold, and make a decision if we want to use the
		// per-row filter for this row group.
		if (lb_word_idx == ub_word_idx) {
			const auto range_mask = ((~0ULL << lb_bit_off) & (~0ULL >> (WORD_MASK - ub_bit_off)));
			if (bitmap[lb_word_idx] & range_mask) {
				return FilterPropagateResult::NO_PRUNING_POSSIBLE;
			}
			return FilterPropagateResult::FILTER_ALWAYS_FALSE;
		}

		const auto lb_word_mask = (~0ULL << lb_bit_off);
		if (bitmap[lb_word_idx] & lb_word_mask) {
			return FilterPropagateResult::NO_PRUNING_POSSIBLE;
		}

		for (idx_t i = NumericCast<idx_t>(lb_word_idx) + 1; i < NumericCast<idx_t>(ub_word_idx); i++) {
			if (bitmap[i]) {
				return FilterPropagateResult::NO_PRUNING_POSSIBLE;
			}
		}

		const auto ub_word_mask = ~0ULL >> (WORD_MASK - ub_bit_off);
		if (bitmap[ub_word_idx] & ub_word_mask) {
			return FilterPropagateResult::NO_PRUNING_POSSIBLE;
		}

		return FilterPropagateResult::FILTER_ALWAYS_FALSE;
	}

	bool IsInitialized() const {
		return initialized;
	}

private:
	static constexpr idx_t MAX_PREFIX_LENGTH = 20;
	static constexpr idx_t CAP_BITS = 1ULL << MAX_PREFIX_LENGTH;
	static constexpr idx_t WORD_SHIFT = 6;
	static constexpr idx_t WORD_MASK = 63;

	bool initialized = false;
	U min;
	U span;
	idx_t shift;
	idx_t word_count;
	AllocatedData buf_;
	uint64_t *bitmap;
};

template <typename T>
struct NumericPrefixPolicy {
	using input_type = T;
	using comparable_type = typename MakeUnsigned<T>::type;

	static comparable_type ToComparable(input_type value) {
		auto result = static_cast<comparable_type>(value);
		if (std::is_signed<T>::value) {
			result ^= comparable_type(1) << ((sizeof(T) * 8) - 1);
		}
		return result;
	}

	static comparable_type ToComparable(const Value &value) {
		return ToComparable(value.GetValueUnsafe<input_type>());
	}
};

struct StringPrefixPolicy {
	using input_type = string_t;
	using comparable_type = uint32_t;

	static comparable_type ToComparable(const input_type &value) {
		return value.GetPrefixIntegerComparable();
	}

	static comparable_type ToComparable(const Value &value) {
		return ToComparable(value.GetValueUnsafe<input_type>());
	}
};

static uint32_t StringStatsMinComparable(const BaseStatistics &stats) {
	return string_t(StringStats::Min(stats)).GetPrefixIntegerComparable();
}

static uint32_t StringStatsMaxComparable(const BaseStatistics &stats) {
	const auto max_string = StringStats::Max(stats);
	if (max_string.size() >= string_t::PREFIX_BYTES) {
		return string_t(max_string).GetPrefixIntegerComparable();
	}

	char padded_prefix[string_t::PREFIX_BYTES];
	memset(padded_prefix, 0xFF, sizeof(padded_prefix));
	memcpy(padded_prefix, max_string.data(), max_string.size());
	return string_t(padded_prefix, string_t::PREFIX_BYTES).GetPrefixIntegerComparable();
}

template <typename Policy>
class TemplatedPrefixRangeFilter : public PrefixRangeFilter {
private:
	using Input = typename Policy::input_type;
	using Comparable = typename Policy::comparable_type;

public:
	void Initialize(ClientContext &context, idx_t number_of_rows, Value min_val, Value max_val) override {
		D_ASSERT(min_val <= max_val);
		D_ASSERT(number_of_rows > 0);
		bitmap.Initialize(context, Policy::ToComparable(min_val), Policy::ToComparable(max_val));
	}

	unique_ptr<BuildState> InitializeBuildState(ClientContext &context) const override {
		return bitmap.InitializeBuildState(context);
	}

	void InsertKeys(Vector &keys, idx_t count, BuildState &state) const override {
		UnifiedVectorFormat vector_data;
		keys.ToUnifiedFormat(count, vector_data);
		const auto data = UnifiedVectorFormat::GetData<const Input>(vector_data);
		const auto &validity_mask = vector_data.validity;

		if (validity_mask.AllValid()) {
			for (idx_t i = 0; i < count; i++) {
				const auto data_idx = vector_data.sel->get_index(i);
				bitmap.Insert(Policy::ToComparable(data[data_idx]), state);
			}
		} else {
			for (idx_t i = 0; i < count; i++) {
				const auto data_idx = vector_data.sel->get_index(i);
				if (!validity_mask.RowIsValidUnsafe(data_idx)) {
					continue;
				}
				bitmap.Insert(Policy::ToComparable(data[data_idx]), state);
			}
		}
	}

	void MergeBuildState(BuildState &state) override {
		bitmap.MergeBuildState(state);
	}

	idx_t LookupKeys(Vector &keys, SelectionVector &result_sel, idx_t count) const override {
		if (keys.GetVectorType() == VectorType::CONSTANT_VECTOR) {
			return LookupOneValue(keys.GetValue(0)) ? count : 0;
		}

		UnifiedVectorFormat vector_data;
		keys.ToUnifiedFormat(count, vector_data);
		const auto data = UnifiedVectorFormat::GetData<const Input>(vector_data);
		const auto &validity_mask = vector_data.validity;

		idx_t found_count = 0;
		if (validity_mask.AllValid()) {
			for (idx_t i = 0; i < count; i++) {
				result_sel.set_index(found_count, i);
				const auto data_idx = vector_data.sel->get_index(i);
				found_count += bitmap.Lookup(Policy::ToComparable(data[data_idx]));
			}
		} else {
			for (idx_t i = 0; i < count; i++) {
				const auto data_idx = vector_data.sel->get_index(i);
				if (!validity_mask.RowIsValidUnsafe(data_idx)) {
					continue;
				}
				result_sel.set_index(found_count, i);
				found_count += bitmap.Lookup(Policy::ToComparable(data[data_idx]));
			}
		}

		return found_count;
	}

	bool LookupOneValue(const Value &key) const override {
		if (key.IsNull()) {
			return false;
		}
		return bitmap.Lookup(Policy::ToComparable(key));
	}

	FilterPropagateResult LookupRange(const Value &lower_bound, const Value &upper_bound) const override {
		return bitmap.LookupRange(Policy::ToComparable(lower_bound), Policy::ToComparable(upper_bound));
	}

	FilterPropagateResult LookupComparableRange(Comparable lower_bound, Comparable upper_bound) const {
		return bitmap.LookupRange(lower_bound, upper_bound);
	}

	bool IsInitialized() const override {
		return bitmap.IsInitialized();
	}

private:
	PrefixRangeBitmap<Comparable> bitmap;
};

template <typename T>
bool ComputeSpan(const Value &lower_bound, const Value &upper_bound, uhugeint_t &result) {
	T lb_value = lower_bound.GetValueUnsafe<T>();
	T ub_value = upper_bound.GetValueUnsafe<T>();
	T res;
	if (TrySubtractOperator::Operation(ub_value, lb_value, res)) {
		result = Uhugeint::Convert(res);
		return true;
	} else {
		return false;
	}
}

bool ComputeStringPrefixSpan(const Value &lower_bound, const Value &upper_bound, uhugeint_t &result) {
#ifdef DUCKDB_DEBUG_NO_INLINE
	return false;
#else
	auto lb_value = lower_bound.GetValueUnsafe<string_t>().GetPrefixIntegerComparable();
	auto ub_value = upper_bound.GetValueUnsafe<string_t>().GetPrefixIntegerComparable();
	result = Uhugeint::Convert(NumericCast<uint64_t>(ub_value - lb_value));
	return true;
#endif
}

} // namespace

unique_ptr<PrefixRangeFilter> PrefixRangeFilter::CreatePrefixRangeFilter(const LogicalType &key_type) {
	switch (key_type.InternalType()) {
	case PhysicalType::UINT8:
		return make_uniq<TemplatedPrefixRangeFilter<NumericPrefixPolicy<uint8_t>>>();
	case PhysicalType::UINT16:
		return make_uniq<TemplatedPrefixRangeFilter<NumericPrefixPolicy<uint16_t>>>();
	case PhysicalType::UINT32:
		return make_uniq<TemplatedPrefixRangeFilter<NumericPrefixPolicy<uint32_t>>>();
	case PhysicalType::UINT64:
		return make_uniq<TemplatedPrefixRangeFilter<NumericPrefixPolicy<uint64_t>>>();
	case PhysicalType::INT8:
		return make_uniq<TemplatedPrefixRangeFilter<NumericPrefixPolicy<int8_t>>>();
	case PhysicalType::INT16:
		return make_uniq<TemplatedPrefixRangeFilter<NumericPrefixPolicy<int16_t>>>();
	case PhysicalType::INT32:
		return make_uniq<TemplatedPrefixRangeFilter<NumericPrefixPolicy<int32_t>>>();
	case PhysicalType::INT64:
		return make_uniq<TemplatedPrefixRangeFilter<NumericPrefixPolicy<int64_t>>>();
	case PhysicalType::VARCHAR:
#ifdef DUCKDB_DEBUG_NO_INLINE
		throw NotImplementedException("Prefix range filter is not implemented for type %s", key_type.ToString());
#else
		return make_uniq<TemplatedPrefixRangeFilter<StringPrefixPolicy>>();
#endif
	case PhysicalType::INT128:
	case PhysicalType::UINT128:
	default:
		throw NotImplementedException("Prefix range filter is not implemented for type %s", key_type.ToString());
	}
}

bool PrefixRangeFilter::TryComputeSpan(const Value &lower_bound, const Value &upper_bound, uhugeint_t &result) {
	if (lower_bound.type().InternalType() != upper_bound.type().InternalType()) {
		return false;
	}

	switch (lower_bound.type().InternalType()) {
	case PhysicalType::UINT8:
		return ComputeSpan<uint8_t>(lower_bound, upper_bound, result);
	case PhysicalType::UINT16:
		return ComputeSpan<uint16_t>(lower_bound, upper_bound, result);
	case PhysicalType::UINT32:
		return ComputeSpan<uint32_t>(lower_bound, upper_bound, result);
	case PhysicalType::UINT64:
		return ComputeSpan<uint64_t>(lower_bound, upper_bound, result);
	case PhysicalType::INT8:
		return ComputeSpan<int8_t>(lower_bound, upper_bound, result);
	case PhysicalType::INT16:
		return ComputeSpan<int16_t>(lower_bound, upper_bound, result);
	case PhysicalType::INT32:
		return ComputeSpan<int32_t>(lower_bound, upper_bound, result);
	case PhysicalType::INT64:
		return ComputeSpan<int64_t>(lower_bound, upper_bound, result);
	case PhysicalType::VARCHAR:
		return ComputeStringPrefixSpan(lower_bound, upper_bound, result);
	case PhysicalType::INT128:
	case PhysicalType::UINT128:
	default:
		return false;
	}
}

bool PrefixRangeTableFilter::SupportedType(const LogicalType &type) {
	switch (type.InternalType()) {
	case PhysicalType::UINT8:
	case PhysicalType::UINT16:
	case PhysicalType::UINT32:
	case PhysicalType::UINT64:
	case PhysicalType::INT8:
	case PhysicalType::INT16:
	case PhysicalType::INT32:
	case PhysicalType::INT64:
		return true;
	case PhysicalType::VARCHAR:
#ifdef DUCKDB_DEBUG_NO_INLINE
		return false;
#else
		return true;
#endif
	case PhysicalType::INT128:
	case PhysicalType::UINT128:
	default:
		return false;
	}
}

PrefixRangeTableFilter::PrefixRangeTableFilter(optional_ptr<PrefixRangeFilter> filter_p,
                                               const string &key_column_name_p, const LogicalType &key_type_p)
    : TableFilter(TYPE), filter(filter_p), key_column_name(key_column_name_p), key_type(key_type_p) {
}
string PrefixRangeTableFilter::ToString(const string &column_name) const {
	return column_name + " IN PRF(" + key_column_name + ")";
}

idx_t PrefixRangeTableFilter::Filter(Vector &keys, SelectionVector &sel, idx_t &approved_tuple_count,
                                     JoinFilterTableFilterState &state) const {
	if (!filter || !filter->IsInitialized()) {
		return approved_tuple_count;
	}

	state.PrepareSlicedKeys(keys, sel, approved_tuple_count);

	const auto approved_before = approved_tuple_count;
	SelectionVector result_sel(approved_before);
	approved_tuple_count = filter->LookupKeys(state.keys_sliced_v, result_sel, approved_before);

	if (approved_tuple_count == approved_before) {
		// Nothing was filtered
		return approved_tuple_count;
	}

	if (sel.IsSet()) {
		for (idx_t idx = 0; idx < approved_tuple_count; idx++) {
			const idx_t sliced_sel_idx = result_sel.get_index_unsafe(idx);
			const idx_t original_sel_idx = sel.get_index_unsafe(sliced_sel_idx);
			sel.set_index(idx, original_sel_idx);
		}
	} else {
		sel.Initialize(result_sel);
	}

	return approved_tuple_count;
}

bool PrefixRangeTableFilter::FilterValue(const Value &value) const {
	if (!filter || !filter->IsInitialized()) {
		return true;
	}

	auto cast_value = value;
	if (!cast_value.DefaultTryCastAs(GetKeyType())) {
		return true;
	}

	Vector keys(cast_value);
	SelectionVector sel;
	idx_t approved_tuple_count = 1;
	return filter->LookupKeys(keys, sel, approved_tuple_count) == 1;
}

FilterPropagateResult PrefixRangeTableFilter::CheckStatistics(BaseStatistics &stats) const {
	if (!filter || !filter->IsInitialized()) {
		return FilterPropagateResult::NO_PRUNING_POSSIBLE;
	}

	switch (stats.GetStatsType()) {
	case StatisticsType::NUMERIC_STATS:
		if (!NumericStats::HasMinMax(stats)) {
			return FilterPropagateResult::NO_PRUNING_POSSIBLE;
		}
		break;
	case StatisticsType::STRING_STATS:
		if (!stats.CanHaveNoNull() || !StringStats::HasMaxStringLength(stats)) {
			return FilterPropagateResult::NO_PRUNING_POSSIBLE;
		}
		break;
	default:
		return FilterPropagateResult::NO_PRUNING_POSSIBLE;
	}

	Value min;
	Value max;
	if (stats.GetStatsType() == StatisticsType::STRING_STATS) {
		const auto min_comparable = StringStatsMinComparable(stats);
		const auto max_comparable = StringStatsMaxComparable(stats);
		auto *string_filter = dynamic_cast<const TemplatedPrefixRangeFilter<StringPrefixPolicy> *>(filter.get());
		D_ASSERT(string_filter);
		if (min_comparable > max_comparable) {
			return FilterPropagateResult::NO_PRUNING_POSSIBLE;
		}
		return string_filter->LookupComparableRange(min_comparable, max_comparable);
	}

	min = NumericStats::Min(stats);
	max = NumericStats::Max(stats);
	if (min > max) {
		return FilterPropagateResult::NO_PRUNING_POSSIBLE;
	}

	// The filter was built with key_type (condition_type), but stats are in storage_type
	if (stats.GetType() != key_type && (!min.DefaultTryCastAs(key_type) || !max.DefaultTryCastAs(key_type))) {
		return FilterPropagateResult::NO_PRUNING_POSSIBLE;
	}

	return filter->LookupRange(min, max);
}

bool PrefixRangeTableFilter::Equals(const TableFilter &other_p) const {
	if (!TableFilter::Equals(other_p)) {
		return false;
	}
	const auto &other = other_p.Cast<PrefixRangeTableFilter>();
	return key_column_name == other.key_column_name && key_type == other.key_type;
}

unique_ptr<TableFilter> PrefixRangeTableFilter::Copy() const {
	return make_uniq<PrefixRangeTableFilter>(this->filter, this->key_column_name, this->key_type);
}

unique_ptr<Expression> PrefixRangeTableFilter::ToExpression(const Expression &column) const {
	auto bound_constant = make_uniq<BoundConstantExpression>(Value(true));
	return std::move(bound_constant);
}

void PrefixRangeTableFilter::Serialize(Serializer &serializer) const {
	TableFilter::Serialize(serializer);
	serializer.WriteProperty<string>(200, "key_column_name", key_column_name);
	serializer.WriteProperty<LogicalType>(201, "key_type", key_type);
}

unique_ptr<TableFilter> PrefixRangeTableFilter::Deserialize(Deserializer &deserializer) {
	auto key_column_name = deserializer.ReadProperty<string>(200, "key_column_name");
	auto key_type = deserializer.ReadProperty<LogicalType>(201, "key_type");

	auto result = make_uniq<PrefixRangeTableFilter>(nullptr, key_column_name, key_type);
	return std::move(result);
}

} // namespace duckdb
