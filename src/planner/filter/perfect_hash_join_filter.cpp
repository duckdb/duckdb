#include "duckdb/planner/filter/perfect_hash_join_filter.hpp"

#include "duckdb/execution/operator/join/perfect_hash_join_executor.hpp"
#include "duckdb/planner/expression/bound_constant_expression.hpp"
#include "duckdb/common/operator/subtract.hpp"
#include "duckdb/common/operator/cast_operators.hpp"
#include "duckdb/planner/table_filter_state.hpp"

namespace duckdb {

PerfectHashJoinFilter::PerfectHashJoinFilter(optional_ptr<const PerfectHashJoinExecutor> perfect_join_executor_p,
                                             const string &key_column_name_p, const LogicalType &key_type_p)
    : TableFilter(TYPE), perfect_join_executor(perfect_join_executor_p), key_column_name(key_column_name_p),
      key_type(key_type_p) {
}

template <class T>
static FilterPropagateResult TemplatedCheckStatistics(const PerfectHashJoinExecutor &perfect_join_executor, T min,
                                                      T max, const LogicalType &type) {
	if (min > max) {
		return FilterPropagateResult::NO_PRUNING_POSSIBLE; // Invalid stats
	}

	T range_typed;
	idx_t range;
	if (!TrySubtractOperator::Operation(max, min, range_typed) || !TryCast::Operation(range_typed, range) ||
	    range >= DEFAULT_STANDARD_VECTOR_SIZE) {
		return FilterPropagateResult::NO_PRUNING_POSSIBLE; // Overflow or too wide of a range
	}

	Vector range_vec(type, DEFAULT_STANDARD_VECTOR_SIZE);
	auto range_data = FlatVector::GetDataMutable<T>(range_vec);
	T val = min;
	for (; val < max; val += 1) {
		*range_data++ = val;
	}
	*range_data = val;

	const auto total_count = NumericCast<idx_t>(range_typed) + 1;
	idx_t approved_tuple_count = 0;
	SelectionVector probe_sel(total_count);
	perfect_join_executor.FillSelectionVectorSwitchProbe(range_vec, total_count, probe_sel, approved_tuple_count,
	                                                     nullptr);

	if (approved_tuple_count == 0) {
		return FilterPropagateResult::FILTER_ALWAYS_FALSE;
	}
	if (approved_tuple_count == total_count) {
		return FilterPropagateResult::FILTER_ALWAYS_TRUE;
	}
	return FilterPropagateResult::NO_PRUNING_POSSIBLE;
}

FilterPropagateResult PerfectHashJoinFilter::CheckStatistics(BaseStatistics &stats) const {
	if (!perfect_join_executor || !TypeIsInteger(key_type.InternalType()) || !NumericStats::HasMinMax(stats)) {
		return FilterPropagateResult::NO_PRUNING_POSSIBLE;
	}

	auto min_val = NumericStats::Min(stats);
	auto max_val = NumericStats::Max(stats);

	// The filter was built with key_type (condition_type), but stats are in storage_type
	if (stats.GetType() != key_type && (!min_val.DefaultTryCastAs(key_type) || !max_val.DefaultTryCastAs(key_type))) {
		return FilterPropagateResult::NO_PRUNING_POSSIBLE;
	}

	switch (key_type.InternalType()) {
	case PhysicalType::UINT8:
		return TemplatedCheckStatistics<uint8_t>(*perfect_join_executor, min_val.GetValueUnsafe<uint8_t>(),
		                                         max_val.GetValueUnsafe<uint8_t>(), key_type);
	case PhysicalType::UINT16:
		return TemplatedCheckStatistics<uint16_t>(*perfect_join_executor, min_val.GetValueUnsafe<uint16_t>(),
		                                          max_val.GetValueUnsafe<uint16_t>(), key_type);
	case PhysicalType::UINT32:
		return TemplatedCheckStatistics<uint32_t>(*perfect_join_executor, min_val.GetValueUnsafe<uint32_t>(),
		                                          max_val.GetValueUnsafe<uint32_t>(), key_type);
	case PhysicalType::UINT64:
		return TemplatedCheckStatistics<uint64_t>(*perfect_join_executor, min_val.GetValueUnsafe<uint64_t>(),
		                                          max_val.GetValueUnsafe<uint64_t>(), key_type);
	case PhysicalType::UINT128:
		return TemplatedCheckStatistics<uhugeint_t>(*perfect_join_executor, min_val.GetValueUnsafe<uhugeint_t>(),
		                                            max_val.GetValueUnsafe<uhugeint_t>(), key_type);
	case PhysicalType::INT8:
		return TemplatedCheckStatistics<int8_t>(*perfect_join_executor, min_val.GetValueUnsafe<int8_t>(),
		                                        max_val.GetValueUnsafe<int8_t>(), key_type);
	case PhysicalType::INT16:
		return TemplatedCheckStatistics<int16_t>(*perfect_join_executor, min_val.GetValueUnsafe<int16_t>(),
		                                         max_val.GetValueUnsafe<int16_t>(), key_type);
	case PhysicalType::INT32:
		return TemplatedCheckStatistics<int32_t>(*perfect_join_executor, min_val.GetValueUnsafe<int32_t>(),
		                                         max_val.GetValueUnsafe<int32_t>(), key_type);
	case PhysicalType::INT64:
		return TemplatedCheckStatistics<int64_t>(*perfect_join_executor, min_val.GetValueUnsafe<int64_t>(),
		                                         max_val.GetValueUnsafe<int64_t>(), key_type);
	case PhysicalType::INT128:
		return TemplatedCheckStatistics<hugeint_t>(*perfect_join_executor, min_val.GetValueUnsafe<hugeint_t>(),
		                                           max_val.GetValueUnsafe<hugeint_t>(), key_type);
	default:
		return FilterPropagateResult::NO_PRUNING_POSSIBLE;
	}
}

string PerfectHashJoinFilter::ToString(const string &column_name) const {
	return column_name + " IN PHJ(" + key_column_name + ")";
}

idx_t PerfectHashJoinFilter::Filter(Vector &keys, SelectionVector &sel, idx_t &approved_tuple_count,
                                    JoinFilterTableFilterState &state) const {
	if (!perfect_join_executor) {
		return approved_tuple_count;
	}

	state.PrepareSlicedKeys(keys, sel, approved_tuple_count);

	// Perform the probe
	const idx_t approved_before = approved_tuple_count;
	approved_tuple_count = 0;
	perfect_join_executor->FillSelectionVectorSwitchProbe(state.keys_sliced_v, approved_before, state.probe_sel,
	                                                      approved_tuple_count, nullptr);

	if (approved_tuple_count == approved_before) {
		return approved_tuple_count; // Nothing was filtered
	}

	if (sel.IsSet()) {
		for (idx_t idx = 0; idx < approved_tuple_count; idx++) {
			const idx_t sliced_sel_idx = state.probe_sel.get_index_unsafe(idx);
			const idx_t original_sel_idx = sel.get_index_unsafe(sliced_sel_idx);
			sel.set_index(idx, original_sel_idx);
		}
	} else {
		sel.Initialize(state.probe_sel);
	}

	return approved_tuple_count;
}

bool PerfectHashJoinFilter::FilterValue(const Value &value) const {
	auto cast_value = value;
	if (!cast_value.DefaultTryCastAs(GetKeyType())) {
		return true;
	}

	Vector keys(cast_value);
	SelectionVector sel;
	const idx_t approved_before = 1;
	idx_t approved_tuple_count = 0;
	perfect_join_executor->FillSelectionVectorSwitchProbe(keys, approved_before, sel, approved_tuple_count, nullptr);

	return approved_tuple_count == 1;
}

bool PerfectHashJoinFilter::Equals(const TableFilter &other_p) const {
	if (!TableFilter::Equals(other_p)) {
		return false;
	}
	auto &other = other_p.Cast<PerfectHashJoinFilter>();
	return perfect_join_executor.get() == other.perfect_join_executor.get() && key_column_name == other.key_column_name;
}
unique_ptr<TableFilter> PerfectHashJoinFilter::Copy() const {
	return make_uniq<PerfectHashJoinFilter>(perfect_join_executor, key_column_name, key_type);
}

unique_ptr<Expression> PerfectHashJoinFilter::ToExpression(const Expression &column) const {
	auto bound_constant = make_uniq<BoundConstantExpression>(Value(true));
	return std::move(bound_constant);
}

void PerfectHashJoinFilter::Serialize(Serializer &serializer) const {
	TableFilter::Serialize(serializer);
	serializer.WriteProperty<string>(200, "key_column_name", key_column_name);
	serializer.WriteProperty<LogicalType>(201, "key_type", key_type);
}

unique_ptr<TableFilter> PerfectHashJoinFilter::Deserialize(Deserializer &deserializer) {
	auto key_column_name = deserializer.ReadProperty<string>(200, "key_column_name");
	auto key_type = deserializer.ReadProperty<LogicalType>(201, "key_type");
	return make_uniq<PerfectHashJoinFilter>(nullptr, key_column_name, key_type);
}

} // namespace duckdb
