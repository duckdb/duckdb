#include "duckdb/planner/filter/prefix_range_filter.hpp"

#include "duckdb/common/assert.hpp"
#include "duckdb/common/enums/filter_propagate_result.hpp"
#include "duckdb/common/enums/vector_type.hpp"
#include "duckdb/common/numeric_utils.hpp"
#include "duckdb/common/optional_ptr.hpp"
#include "duckdb/common/typedefs.hpp"
#include "duckdb/common/types/selection_vector.hpp"
#include "duckdb/common/types/vector.hpp"
#include "duckdb/planner/expression/bound_constant_expression.hpp"

namespace duckdb {

namespace {

template <typename T>
idx_t CountLeadingZeros(const T &v) {
	D_ASSERT(std::is_unsigned<T>());
	const idx_t bit_size = sizeof(v) * 8;
	if (v == 0) {
		return bit_size;
	}

	idx_t count = 0;
	T mask = T(1) << (bit_size - 1);

	while ((v & mask) == 0) {
		++count;
		mask >>= 1;
	}
	return count;
}

template <typename T>
class NumericPrefixRangeFilter : public PrefixRangeFilter {
private:
	using U = typename MakeUnsigned<T>::type;

	struct NumericBuildState : public PrefixRangeFilter::BuildState {
		explicit NumericBuildState(AllocatedData data_p, uint64_t *bitmap_p, idx_t word_count_p)
		    : data(std::move(data_p)), bitmap(bitmap_p), word_count(word_count_p) {
		}

		AllocatedData data;
		uint64_t *bitmap;
		idx_t word_count;
	};

public:
	void Initialize(ClientContext &context, idx_t number_of_rows, Value min_val, Value max_val) override {
		D_ASSERT(min_val <= max_val);
		D_ASSERT(number_of_rows > 0);
		min = static_cast<U>(min_val.GetValueUnsafe<T>());
		span = (static_cast<U>(max_val.GetValueUnsafe<T>()) - min);
		shift = 0;

		if (span >= CAP_BITS) {
			const auto q = static_cast<uint64_t>((span /* + CAP_BITS*/) >> MAX_PREFIX_LENGTH); // ceil(span/CAP_BITS
			shift = (q <= 1) ? 0 : (64 - CountLeadingZeros(q - 1));                            // ceil_log2(q)
		}

		const idx_t buckets = (span >> shift) + 1;
		word_count = buckets == 0 ? 1 : (buckets + 63) >> 6;
		const idx_t bitmap_bit_size = word_count * sizeof(uint64_t) * 8;

		BufferManager &buffer_manager = BufferManager::GetBufferManager(context);
		buf_ = buffer_manager.GetBufferAllocator().Allocate(64ULL + bitmap_bit_size);
		bitmap = reinterpret_cast<uint64_t *>((64ULL + reinterpret_cast<uint64_t>(buf_.get())) & ~63ULL);
		std::fill_n(bitmap, word_count, 0);

		initialized = false;
	}

	unique_ptr<BuildState> InitializeBuildState(ClientContext &context) const override {
		D_ASSERT(bitmap);
		BufferManager &buffer_manager = BufferManager::GetBufferManager(context);
		auto state_data = buffer_manager.GetBufferAllocator().Allocate(64ULL + word_count * sizeof(uint64_t) * 8);
		auto state_bitmap =
		    reinterpret_cast<uint64_t *>((64ULL + reinterpret_cast<uint64_t>(state_data.get())) & ~63ULL);
		std::fill_n(state_bitmap, word_count, 0);
		return make_uniq<NumericBuildState>(std::move(state_data), state_bitmap, word_count);
	}

	void InsertKeys(Vector &keys, idx_t count, BuildState &state_p) const override {
		auto &state = static_cast<NumericBuildState &>(state_p);
		UnifiedVectorFormat vector_data;
		keys.ToUnifiedFormat(count, vector_data);
		const auto key_data = UnifiedVectorFormat::GetData<const T>(vector_data);
		const auto &validity_mask = vector_data.validity;

		if (validity_mask.AllValid()) {
			for (idx_t i = 0; i < count; i++) {
				const idx_t data_idx = vector_data.sel->get_index(i);
				const U &key = static_cast<U>(key_data[data_idx]);
				const U y = key - min;
				// All x are in-range by construction, so range check can be omitted here.
				const U idx = y >> shift;
				state.bitmap[idx >> 6] |= 1ULL << (idx & 63u);
			}
		} else {
			for (idx_t i = 0; i < count; i++) {
				const auto data_idx = vector_data.sel->get_index(i);
				if (!validity_mask.RowIsValidUnsafe(data_idx)) {
					continue;
				}
				const U &key = static_cast<U>(key_data[data_idx]);
				const U y = key - min;
				// All x are in-range by construction, so range check can be omitted here.
				const U idx = y >> shift;
				state.bitmap[idx >> 6] |= 1ULL << (idx & 63u);
			}
		}
	}

	void MergeBuildState(BuildState &state_p) override {
		auto &state = static_cast<NumericBuildState &>(state_p);
		for (idx_t word_idx = 0; word_idx < word_count; word_idx++) {
			bitmap[word_idx] |= state.bitmap[word_idx];
		}
		initialized = true;
	}

	inline idx_t LookupOne(const T &k) const {
		const U &key = static_cast<U>(k);
		const U y = key - min;
		const U bit_idx = y >> shift;
		const uint8_t in_range = y <= span;
		const uint32_t word_idx = (bit_idx >> 6) & (0U - in_range);
		const uint8_t bit = (bitmap[word_idx] >> (bit_idx & 63ULL)) & 1ULL;
		return bit & in_range;
	}

	idx_t LookupKeys(Vector &keys, SelectionVector &result_sel, idx_t count) const override {
		if (keys.GetVectorType() == VectorType::CONSTANT_VECTOR) {
			return LookupOneValue(keys.GetValue(0)) ? count : 0;
		}

		UnifiedVectorFormat vector_data;
		keys.ToUnifiedFormat(count, vector_data);
		const auto data = UnifiedVectorFormat::GetData<const T>(vector_data);
		const auto &validity_mask = vector_data.validity;

		idx_t found_count = 0;
		if (validity_mask.AllValid()) {
			for (idx_t i = 0; i < count; i++) {
				result_sel.set_index(found_count, i);
				const auto data_idx = vector_data.sel->get_index(i);
				const auto &key = data[data_idx];
				found_count += LookupOne(key);
			}
		} else {
			for (idx_t i = 0; i < count; i++) {
				const auto data_idx = vector_data.sel->get_index(i);
				if (!validity_mask.RowIsValidUnsafe(data_idx)) {
					continue;
				}
				result_sel.set_index(found_count, i);
				const auto &key = data[data_idx];
				found_count += LookupOne(key);
			}
		}

		return found_count;
	}

	bool LookupOneValue(const Value &key) const override {
		return LookupOne(key.GetValueUnsafe<T>());
	}

	FilterPropagateResult LookupRange(const Value &lower_bound, const Value &upper_bound) const override {
		const auto lb = lower_bound.GetValueUnsafe<T>();
		const auto ub = upper_bound.GetValueUnsafe<T>();

		const auto min_t = static_cast<T>(min);
		const auto max_t = static_cast<T>(min + span);
		if (ub < min_t || lb > max_t) {
			return FilterPropagateResult::FILTER_ALWAYS_FALSE;
		}

		const auto adjusted_lb = static_cast<U>(std::max(lb, min_t));
		const auto adjusted_ub = static_cast<U>(std::min(ub, max_t));

		const auto lb_y = static_cast<U>(adjusted_lb - static_cast<U>(min_t));
		const U lb_bit_idx = lb_y >> shift;
		const U lb_word_idx = lb_bit_idx >> 6;

		const auto ub_y = static_cast<U>(adjusted_ub - static_cast<U>(min_t));
		const U ub_bit_idx = ub_y >> shift;
		const U ub_word_idx = ub_bit_idx >> 6;

		const auto lb_bit_off = lb_bit_idx & 63u;
		const auto ub_bit_off = ub_bit_idx & 63u;

		if (lb_word_idx == ub_word_idx) {
			const auto range_mask = ((~0ULL << lb_bit_off) & (~0ULL >> (63u - ub_bit_off)));
			if (bitmap[lb_word_idx] & range_mask) {
				return FilterPropagateResult::NO_PRUNING_POSSIBLE;
			}
			return FilterPropagateResult::FILTER_ALWAYS_FALSE;
		}

		const auto lb_word_mask = (~0ULL << lb_bit_off);
		if (bitmap[lb_word_idx] & lb_word_mask) {
			return FilterPropagateResult::NO_PRUNING_POSSIBLE;
		}

		for (idx_t i = static_cast<idx_t>(lb_word_idx) + 1; i < static_cast<idx_t>(ub_word_idx); i++) {
			if (bitmap[i]) {
				return FilterPropagateResult::NO_PRUNING_POSSIBLE;
			}
		}

		const auto ub_word_mask = ~0ULL >> (63u - ub_bit_off);
		if (bitmap[ub_word_idx] & ub_word_mask) {
			return FilterPropagateResult::NO_PRUNING_POSSIBLE;
		}

		return FilterPropagateResult::FILTER_ALWAYS_FALSE;
	}

	bool IsInitialized() const override {
		return initialized;
	}

private:
	static constexpr idx_t MAX_PREFIX_LENGTH = 20;
	static constexpr size_t CAP_BITS = 1ULL << MAX_PREFIX_LENGTH;

	bool initialized = false;
	U min;
	U span;
	idx_t shift;
	idx_t word_count;
	AllocatedData buf_;
	uint64_t *bitmap;
};

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
	case PhysicalType::INT128:
	case PhysicalType::UINT128:
	default:
		throw NotImplementedException("Prefix range filter is not implemented for type %s", key_type.ToString());
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
	case PhysicalType::INT128:
	case PhysicalType::UINT128:
	default:
		return false;
	}
}

PrefixRangeTableFilter::PrefixRangeTableFilter(optional_ptr<PrefixRangeFilter> filter_p,
                                               const bool filters_null_values_p, const string &key_column_name_p,
                                               const LogicalType &key_type_p)
    : TableFilter(TYPE), filter(filter_p), filters_null_values(filters_null_values_p),
      key_column_name(key_column_name_p), key_type(key_type_p) {
}
string PrefixRangeTableFilter::ToString(const string &column_name) const {
	return column_name + " IN PRF(" + key_column_name + ")";
}

idx_t PrefixRangeTableFilter::Filter(Vector &keys, SelectionVector &sel, idx_t &approved_tuple_count) const {
	if (!filter || !filter->IsInitialized()) {
		return approved_tuple_count;
	}

	const auto approved_before = approved_tuple_count;
	Vector keys_sliced(keys, sel, approved_before);
	SelectionVector result_sel(approved_before);
	approved_tuple_count = filter->LookupKeys(keys_sliced, result_sel, approved_before);

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
	return filter->LookupOneValue(value);
}

FilterPropagateResult PrefixRangeTableFilter::CheckStatistics(BaseStatistics &stats) const {
	if (!filter || !filter->IsInitialized()) {
		return FilterPropagateResult::NO_PRUNING_POSSIBLE;
	}
	D_ASSERT(stats.GetStatsType() == StatisticsType::NUMERIC_STATS);
	if (!NumericStats::HasMinMax(stats)) {
		return FilterPropagateResult::NO_PRUNING_POSSIBLE;
	}

	const auto min = NumericStats::Min(stats);
	const auto max = NumericStats::Max(stats);
	if (min > max) {
		return FilterPropagateResult::NO_PRUNING_POSSIBLE;
	}

	return filter->LookupRange(min, max);
}

bool PrefixRangeTableFilter::Equals(const TableFilter &other_p) const {
	if (!TableFilter::Equals(other_p)) {
		return false;
	}
	const auto &other = other_p.Cast<PrefixRangeTableFilter>();
	return key_column_name == other.key_column_name && key_type == other.key_type &&
	       filters_null_values == other.filters_null_values;
}

unique_ptr<TableFilter> PrefixRangeTableFilter::Copy() const {
	return make_uniq<PrefixRangeTableFilter>(this->filter, this->filters_null_values, this->key_column_name,
	                                         this->key_type);
}

unique_ptr<Expression> PrefixRangeTableFilter::ToExpression(const Expression &column) const {
	auto bound_constant = make_uniq<BoundConstantExpression>(Value(true));
	return std::move(bound_constant);
}

void PrefixRangeTableFilter::Serialize(Serializer &serializer) const {
	TableFilter::Serialize(serializer);
	serializer.WriteProperty<bool>(200, "filters_null_values", filters_null_values);
	serializer.WriteProperty<string>(201, "key_column_name", key_column_name);
	serializer.WriteProperty<LogicalType>(202, "key_type", key_type);
}

unique_ptr<TableFilter> PrefixRangeTableFilter::Deserialize(Deserializer &deserializer) {
	auto filters_null_values = deserializer.ReadProperty<bool>(200, "filters_null_values");
	auto key_column_name = deserializer.ReadProperty<string>(201, "key_column_name");
	auto key_type = deserializer.ReadProperty<LogicalType>(202, "key_type");

	auto result = make_uniq<PrefixRangeTableFilter>(nullptr, filters_null_values, key_column_name, key_type);
	return std::move(result);
}

} // namespace duckdb
