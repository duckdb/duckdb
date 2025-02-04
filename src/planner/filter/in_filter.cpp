#include "duckdb/planner/filter/in_filter.hpp"

#include "duckdb/storage/statistics/base_statistics.hpp"
#include "duckdb/planner/expression/bound_constant_expression.hpp"
#include "duckdb/planner/expression/bound_operator_expression.hpp"

namespace duckdb {

InFilter::InFilter(vector<Value> values_p) : TableFilter(TableFilterType::IN_FILTER), values(std::move(values_p)) {
	for (auto &val : values) {
		if (val.IsNull()) {
			throw InternalException("InFilter constant cannot be NULL - use IsNullFilter instead");
		}
	}
	for (idx_t i = 1; i < values.size(); i++) {
		if (values[0].type() != values[i].type()) {
			throw InternalException("InFilter constants must all have the same type");
		}
	}
	if (values.empty()) {
		throw InternalException("InFilter constants cannot be empty");
	}
}

FilterPropagateResult InFilter::CheckStatistics(BaseStatistics &stats) {
	switch (values[0].type().InternalType()) {
	case PhysicalType::UINT8:
	case PhysicalType::UINT16:
	case PhysicalType::UINT32:
	case PhysicalType::UINT64:
	case PhysicalType::UINT128:
	case PhysicalType::INT8:
	case PhysicalType::INT16:
	case PhysicalType::INT32:
	case PhysicalType::INT64:
	case PhysicalType::INT128:
	case PhysicalType::FLOAT:
	case PhysicalType::DOUBLE:
		return NumericStats::CheckZonemap(stats, ExpressionType::COMPARE_EQUAL,
		                                  array_ptr<Value>(values.data(), values.size()));
	case PhysicalType::VARCHAR:
		return StringStats::CheckZonemap(stats, ExpressionType::COMPARE_EQUAL,
		                                 array_ptr<Value>(values.data(), values.size()));
	default:
		return FilterPropagateResult::NO_PRUNING_POSSIBLE;
	}
}

string InFilter::ToString(const string &column_name) {
	string in_list;
	for (auto &val : values) {
		if (!in_list.empty()) {
			in_list += ", ";
		}
		in_list += val.ToSQLString();
	}
	return column_name + " IN (" + in_list + ")";
}

unique_ptr<Expression> InFilter::ToExpression(const Expression &column) const {
	auto result = make_uniq<BoundOperatorExpression>(ExpressionType::COMPARE_IN, LogicalType::BOOLEAN);
	result->children.push_back(column.Copy());
	for (auto &val : values) {
		result->children.push_back(make_uniq<BoundConstantExpression>(val));
	}
	return std::move(result);
}

bool InFilter::Equals(const TableFilter &other_p) const {
	if (!TableFilter::Equals(other_p)) {
		return false;
	}
	auto &other = other_p.Cast<InFilter>();
	return other.values == values;
}

unique_ptr<TableFilter> InFilter::Copy() const {
	return make_uniq<InFilter>(values);
}

} // namespace duckdb
