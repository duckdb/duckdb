#include "duckdb/planner/filter/prefix_range_filter.hpp"

#include <stdint.h>
#include <algorithm>
#include <array>
#include <utility>

#include "duckdb/common/allocator.hpp"
#include "duckdb/common/assert.hpp"
#include "duckdb/common/bit_utils.hpp"
#include "duckdb/common/enums/filter_propagate_result.hpp"
#include "duckdb/common/enums/vector_type.hpp"
#include "duckdb/common/helper.hpp"
#include "duckdb/common/numeric_utils.hpp"
#include "duckdb/common/operator/subtract.hpp"
#include "duckdb/common/optional_ptr.hpp"
#include "duckdb/common/typedefs.hpp"
#include "duckdb/common/types.hpp"
#include "duckdb/common/types/selection_vector.hpp"
#include "duckdb/common/types/uhugeint.hpp"
#include "duckdb/common/types/vector.hpp"
#include "duckdb/common/uhugeint.hpp"
#include "duckdb/planner/expression/bound_constant_expression.hpp"
#include "duckdb/storage/statistics/string_stats.hpp"
#include "duckdb/storage/buffer_manager.hpp"
#include "duckdb/planner/table_filter_state.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/serializer/deserializer.hpp"
#include "duckdb/common/serializer/serializer.hpp"
#include "duckdb/common/types/string_type.hpp"
#include "duckdb/common/vector/vector_iterator.hpp"
#include "duckdb/storage/statistics/base_statistics.hpp"
#include "duckdb/storage/statistics/numeric_stats.hpp"

namespace duckdb {
class ClientContext;

namespace {

AllocatedData AllocateBitmap(ClientContext &context, const idx_t word_count, uint64_t *&bitmap_begin) {
	const idx_t size = word_count * sizeof(uint64_t);
	BufferManager &buffer_manager = BufferManager::GetBufferManager(context);
	auto buffer = buffer_manager.GetBufferAllocator().Allocate(64ULL + size);
	bitmap_begin = reinterpret_cast<uint64_t *>((64ULL + reinterpret_cast<uint64_t>(buffer.get())) & ~63ULL);
	std::fill_n(bitmap_begin, word_count, 0);
	return buffer;
}

struct PrefixRangeBitmapBuildState : public PrefixRangeFilter::BuildState {
	explicit PrefixRangeBitmapBuildState(AllocatedData data_p, uint64_t *bitmap_p)
	    : data(std::move(data_p)), bitmap(bitmap_p) {
	}

	AllocatedData data;
	uint64_t *bitmap;
};

template <typename U>
class PrefixRangeBitmap {
public:
	void Initialize(ClientContext &context, U min_p, U span_p) {
		min = min_p;
		span = span_p;
		shift = 0;

		if (span >= CAP_BITS) {
			const auto q = UnsafeNumericCast<uint64_t>(span >> MAX_PREFIX_LENGTH);
			shift = (q <= 1) ? 0 : (64 - CountZeros<uint64_t>::Leading(q - 1));
		}

		const idx_t buckets = UnsafeNumericCast<idx_t>((span >> shift) + 1);
		word_count = buckets == 0 ? 1 : (buckets + 63) >> WORD_SHIFT;

		buf_ = AllocateBitmap(context, word_count, bitmap);

		// Only mark initialized as true when local bitmaps are merged.
		initialized = false;
	}

	unique_ptr<PrefixRangeBitmapBuildState> InitializeBuildState(ClientContext &context) const {
		D_ASSERT(bitmap);
		uint64_t *state_bitmap;
		auto state_data = AllocateBitmap(context, word_count, state_bitmap);
		return make_uniq<PrefixRangeBitmapBuildState>(std::move(state_data), state_bitmap);
	}

	template <typename T, typename CONVERTER>
	void InsertKeys(Vector &keys, idx_t count, uint64_t *state_bitmap) const {
		for (const auto &entry : keys.template ValidValues<T>(count)) {
			const U y = CONVERTER::Convert(entry.value) - min;
			// All keys are in-range by construction, so the range check can be omitted here.
			const U idx = y >> shift;
			state_bitmap[idx >> WORD_SHIFT] |= 1ULL << (idx & WORD_MASK);
		}
	}

	void MergeBuildState(PrefixRangeBitmapBuildState &state) {
		for (idx_t word_idx = 0; word_idx < word_count; word_idx++) {
			bitmap[word_idx] |= state.bitmap[word_idx];
		}
		initialized = true;
	}

	template <typename T, typename CONVERTER>
	inline bool LookupOne(const Value &value) const {
		if (value.IsNull()) {
			return false;
		}

		const U comparable = CONVERTER::Convert(value.GetValueUnsafe<T>());
		const U y = comparable - min;
		const U bit_idx = y >> shift;
		const uint8_t in_range = y <= span;
		const uint32_t word_idx = (bit_idx >> WORD_SHIFT) & (0U - in_range);
		const uint8_t bit = (bitmap[word_idx] >> (bit_idx & WORD_MASK)) & 1ULL;
		return bit & in_range;
	}

	template <typename T, typename CONVERTER>
	idx_t LookupKeys(Vector &keys, SelectionVector &result_sel, idx_t count) const {
		idx_t found_count = 0;
		for (const auto &entry : keys.template ValidValues<T>(count)) {
			const U comparable = CONVERTER::Convert(entry.value);
			const U y = comparable - min;
			const U bit_idx = y >> shift;
			const uint8_t in_range = y <= span;
			const uint32_t word_idx = (bit_idx >> WORD_SHIFT) & (0U - in_range);
			const uint8_t bit = (bitmap[word_idx] >> (bit_idx & WORD_MASK)) & 1ULL;

			result_sel.set_index(found_count, entry.index);
			found_count += bit & in_range;
		}
		return found_count;
	}

	FilterPropagateResult LookupRange(U lower_bound, U upper_bound) const {
		const U lb_y = lower_bound - min;
		const U lb_bit_idx = lb_y >> shift;
		const auto lb_word_idx = lb_bit_idx >> WORD_SHIFT;

		const U ub_y = upper_bound - min;
		const U ub_bit_idx = ub_y >> shift;
		const auto ub_word_idx = ub_bit_idx >> WORD_SHIFT;

		const idx_t lb_bit_off = UnsafeNumericCast<idx_t>(lb_bit_idx & UnsafeNumericCast<U>(WORD_MASK));
		const idx_t ub_bit_off = UnsafeNumericCast<idx_t>(ub_bit_idx & UnsafeNumericCast<U>(WORD_MASK));

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

		for (idx_t i = UnsafeNumericCast<idx_t>(lb_word_idx) + 1; i < UnsafeNumericCast<idx_t>(ub_word_idx); i++) {
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

	U Min() const {
		return min;
	}

	U Span() const {
		return span;
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
struct NumericConverter {
	using comparable_type = typename MakeUnsigned<T>::type;

	static inline comparable_type Convert(T value) {
		// Overflow is explicitly allowed for unsigned to signed cast
		return static_cast<comparable_type>(value);
	}
};

struct StringPrefixConverter {
	static inline uint32_t Convert(const string_t &value) {
		return value.GetPrefixIntegerComparable();
	}
};

uint32_t StringMinComparable(const Value &value) {
	return StringPrefixConverter::Convert(value.GetValueUnsafe<string_t>());
}

uint32_t StringMaxComparable(const Value &value) {
	const auto max_string = value.GetValueUnsafe<string_t>();
	if (max_string.GetSize() >= string_t::PREFIX_BYTES) {
		return max_string.GetPrefixIntegerComparable();
	}

	// Pad string prefix with 0xFF to keep correctness if max is truncated at \0 char, e.g., ab\0c -> ab
	std::array<char, string_t::PREFIX_BYTES> padded_prefix;
	padded_prefix.fill(char(0xFF));
	for (idx_t i = 0; i < max_string.GetSize(); i++) {
		padded_prefix[i] = max_string.GetData()[i];
	}
	return string_t(padded_prefix.data(), string_t::PREFIX_BYTES).GetPrefixIntegerComparable();
}

template <typename T>
class NumericPrefixRangeFilter : public PrefixRangeFilter {
private:
	using Comparable = typename MakeUnsigned<T>::type;

public:
	void Initialize(ClientContext &context, idx_t number_of_rows, Value min_val, Value max_val) override {
		D_ASSERT(min_val <= max_val);
		D_ASSERT(number_of_rows > 0);
		const auto min = NumericConverter<T>::Convert(min_val.GetValueUnsafe<T>());
		const auto max = NumericConverter<T>::Convert(max_val.GetValueUnsafe<T>());
		bitmap.Initialize(context, min, max - min);
	}

	unique_ptr<BuildState> InitializeBuildState(ClientContext &context) const override {
		return bitmap.InitializeBuildState(context);
	}

	void InsertKeys(Vector &keys, idx_t count, BuildState &state) const override {
		auto &bitmap_state = state.Cast<PrefixRangeBitmapBuildState>();
		bitmap.template InsertKeys<T, NumericConverter<T>>(keys, count, bitmap_state.bitmap);
	}

	void MergeBuildState(BuildState &state) override {
		bitmap.MergeBuildState(state.Cast<PrefixRangeBitmapBuildState>());
	}

	idx_t LookupKeys(Vector &keys, SelectionVector &result_sel, idx_t count) const override {
		if (keys.GetVectorType() == VectorType::CONSTANT_VECTOR) {
			return bitmap.template LookupOne<T, NumericConverter<T>>(keys.GetValue(0)) ? count : 0;
		}
		return bitmap.template LookupKeys<T, NumericConverter<T>>(keys, result_sel, count);
	}

	FilterPropagateResult LookupRange(const Value &lower_bound, const Value &upper_bound) const override {
		const auto lb = lower_bound.GetValueUnsafe<T>();
		const auto ub = upper_bound.GetValueUnsafe<T>();

		const auto bitmap_min = static_cast<T>(bitmap.Min());
		const auto bitmap_max = static_cast<T>(bitmap.Min() + bitmap.Span());
		if (ub < bitmap_min || lb > bitmap_max) {
			return FilterPropagateResult::FILTER_ALWAYS_FALSE;
		}

		const auto adjusted_lb = NumericConverter<T>::Convert(MaxValue<T>(lb, bitmap_min));
		const auto adjusted_ub = NumericConverter<T>::Convert(MinValue<T>(ub, bitmap_max));
		return bitmap.LookupRange(adjusted_lb, adjusted_ub);
	}

	bool IsInitialized() const override {
		return bitmap.IsInitialized();
	}

private:
	PrefixRangeBitmap<Comparable> bitmap;
};

class StringPrefixRangeFilter : public PrefixRangeFilter {
public:
	void Initialize(ClientContext &context, idx_t number_of_rows, Value min_val, Value max_val) override {
		D_ASSERT(min_val <= max_val);
		D_ASSERT(number_of_rows > 0);
		const auto min = StringPrefixConverter::Convert(min_val.GetValueUnsafe<string_t>());
		const auto max = StringPrefixConverter::Convert(max_val.GetValueUnsafe<string_t>());
		D_ASSERT(min <= max);
		bitmap.Initialize(context, min, max - min);
	}

	unique_ptr<BuildState> InitializeBuildState(ClientContext &context) const override {
		return bitmap.InitializeBuildState(context);
	}

	void InsertKeys(Vector &keys, idx_t count, BuildState &state) const override {
		auto &bitmap_state = state.Cast<PrefixRangeBitmapBuildState>();
		bitmap.template InsertKeys<string_t, StringPrefixConverter>(keys, count, bitmap_state.bitmap);
	}

	void MergeBuildState(BuildState &state) override {
		bitmap.MergeBuildState(state.Cast<PrefixRangeBitmapBuildState>());
	}

	idx_t LookupKeys(Vector &keys, SelectionVector &result_sel, idx_t count) const override {
		if (keys.GetVectorType() == VectorType::CONSTANT_VECTOR) {
			return bitmap.template LookupOne<string_t, StringPrefixConverter>(keys.GetValue(0)) ? count : 0;
		}
		return bitmap.template LookupKeys<string_t, StringPrefixConverter>(keys, result_sel, count);
	}

	FilterPropagateResult LookupRange(const Value &lower_bound, const Value &upper_bound) const override {
		auto lower_bound_comparable = StringMinComparable(lower_bound);
		auto upper_bound_comparable = StringMaxComparable(upper_bound);
		if (lower_bound_comparable > upper_bound_comparable) {
			return FilterPropagateResult::NO_PRUNING_POSSIBLE;
		}

		const auto bitmap_min = bitmap.Min();
		const auto bitmap_max = bitmap.Min() + bitmap.Span();
		if (upper_bound_comparable < bitmap_min || lower_bound_comparable > bitmap_max) {
			return FilterPropagateResult::FILTER_ALWAYS_FALSE;
		}

		lower_bound_comparable = MaxValue<uint32_t>(lower_bound_comparable, bitmap_min);
		upper_bound_comparable = MinValue<uint32_t>(upper_bound_comparable, bitmap_max);
		return bitmap.LookupRange(lower_bound_comparable, upper_bound_comparable);
	}

	bool IsInitialized() const override {
		return bitmap.IsInitialized();
	}

private:
	PrefixRangeBitmap<uint32_t> bitmap;
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
	uint32_t res;
	if (TrySubtractOperator::Operation(ub_value, lb_value, res)) {
		result = Uhugeint::Convert(res);
		return true;
	} else {
		return false;
	}
#endif
}

} // namespace

unique_ptr<PrefixRangeFilter> PrefixRangeFilter::CreatePrefixRangeFilter(const LogicalType &key_type) {
	switch (key_type.InternalType()) {
	case PhysicalType::UINT8:
		return make_uniq<NumericPrefixRangeFilter<uint8_t>>();
	case PhysicalType::UINT16:
		return make_uniq<NumericPrefixRangeFilter<uint16_t>>();
	case PhysicalType::UINT32:
		return make_uniq<NumericPrefixRangeFilter<uint32_t>>();
	case PhysicalType::UINT64:
		return make_uniq<NumericPrefixRangeFilter<uint64_t>>();
	case PhysicalType::INT8:
		return make_uniq<NumericPrefixRangeFilter<int8_t>>();
	case PhysicalType::INT16:
		return make_uniq<NumericPrefixRangeFilter<int16_t>>();
	case PhysicalType::INT32:
		return make_uniq<NumericPrefixRangeFilter<int32_t>>();
	case PhysicalType::INT64:
		return make_uniq<NumericPrefixRangeFilter<int64_t>>();
	case PhysicalType::VARCHAR:
#ifdef DUCKDB_DEBUG_NO_INLINE
		throw NotImplementedException("Prefix range filter is not implemented for type %s", key_type.ToString());
#else
		return make_uniq<StringPrefixRangeFilter>();
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

	Value min;
	Value max;
	switch (stats.GetStatsType()) {
	case StatisticsType::NUMERIC_STATS:
		if (!NumericStats::HasMinMax(stats)) {
			return FilterPropagateResult::NO_PRUNING_POSSIBLE;
		}
		min = NumericStats::Min(stats);
		max = NumericStats::Max(stats);
		break;
	case StatisticsType::STRING_STATS:
		if (stats.GetType().id() != LogicalTypeId::VARCHAR || key_type.id() != LogicalTypeId::VARCHAR) {
			return FilterPropagateResult::NO_PRUNING_POSSIBLE;
		}
		if (!stats.CanHaveNoNull() || !StringStats::HasMaxStringLength(stats)) {
			return FilterPropagateResult::NO_PRUNING_POSSIBLE;
		}
		min = Value(StringStats::Min(stats));
		max = Value(StringStats::Max(stats));
		break;
	default:
		return FilterPropagateResult::NO_PRUNING_POSSIBLE;
	}

	if (min > max) {
		return FilterPropagateResult::NO_PRUNING_POSSIBLE;
	}

	// The filter was built with key_type (condition_type), but stats are in storage_type.
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
