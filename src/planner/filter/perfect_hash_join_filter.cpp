#include "duckdb/planner/filter/perfect_hash_join_filter.hpp"

#include "duckdb/execution/operator/join/perfect_hash_join_executor.hpp"
#include "duckdb/planner/expression/bound_function_expression.hpp"
#include "duckdb/planner/filter/table_filter_functions.hpp"
#include "duckdb/common/operator/subtract.hpp"
#include "duckdb/common/operator/cast_operators.hpp"

namespace duckdb {

PerfectHashJoinFilter::PerfectHashJoinFilter(optional_ptr<const PerfectHashJoinExecutor> perfect_join_executor_p,
                                             const string &key_column_name_p)
    : TableFilter(TYPE), perfect_join_executor(perfect_join_executor_p), key_column_name(key_column_name_p) {
}

template <class T>
static FilterPropagateResult TemplatedCheckStatistics(const PerfectHashJoinExecutor &perfect_join_executor,
                                                      const BaseStatistics &stats) {
	if (!NumericStats::HasMinMax(stats)) {
		return FilterPropagateResult::NO_PRUNING_POSSIBLE;
	}

	const auto min = NumericStats::GetMin<T>(stats);
	const auto max = NumericStats::GetMax<T>(stats);
	if (min > max) {
		return FilterPropagateResult::NO_PRUNING_POSSIBLE; // Invalid stats
	}

	T range_typed;
	idx_t range;
	if (!TrySubtractOperator::Operation(max, min, range_typed) || !TryCast::Operation(range_typed, range) ||
	    range >= DEFAULT_STANDARD_VECTOR_SIZE) {
		return FilterPropagateResult::NO_PRUNING_POSSIBLE; // Overflow or too wide of a range
	}

	Vector range_vec(stats.GetType(), DEFAULT_STANDARD_VECTOR_SIZE);
	auto range_data = FlatVector::GetData<T>(range_vec);
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
	TableFilter::ThrowDeprecated("PerfectHashJoinFilter");
}

string PerfectHashJoinFilter::ToString(const string &column_name) const {
	TableFilter::ThrowDeprecated("PerfectHashJoinFilter");
}

idx_t PerfectHashJoinFilter::Filter(Vector &keys, SelectionVector &sel, idx_t &approved_tuple_count) const {
	TableFilter::ThrowDeprecated("PerfectHashJoinFilter");
}

bool PerfectHashJoinFilter::FilterValue(const Value &value) const {
	TableFilter::ThrowDeprecated("PerfectHashJoinFilter");
}

bool PerfectHashJoinFilter::Equals(const TableFilter &other_p) const {
	TableFilter::ThrowDeprecated("PerfectHashJoinFilter");
}
unique_ptr<TableFilter> PerfectHashJoinFilter::Copy() const {
	TableFilter::ThrowDeprecated("PerfectHashJoinFilter");
}

unique_ptr<Expression> PerfectHashJoinFilter::ToExpression(const Expression &column) const {
	auto func = PerfectHashJoinScalarFun::GetFunction(column.return_type);
	auto bind_data = make_uniq<PerfectHashJoinFunctionData>(perfect_join_executor, key_column_name, 0.0f, idx_t(0));
	vector<unique_ptr<Expression>> args;
	args.push_back(column.Copy());
	return make_uniq<BoundFunctionExpression>(LogicalType::BOOLEAN, std::move(func), std::move(args),
	                                          std::move(bind_data));
}

void PerfectHashJoinFilter::Serialize(Serializer &serializer) const {
	TableFilter::Serialize(serializer);
	serializer.WriteProperty<string>(200, "key_column_name", key_column_name);
}

unique_ptr<TableFilter> PerfectHashJoinFilter::Deserialize(Deserializer &deserializer) {
	auto key_column_name = deserializer.ReadProperty<string>(200, "key_column_name");
	return make_uniq<PerfectHashJoinFilter>(nullptr, key_column_name);
}

} // namespace duckdb
