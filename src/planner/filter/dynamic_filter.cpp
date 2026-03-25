#include "duckdb/planner/filter/dynamic_filter.hpp"
#include "duckdb/common/enum_util.hpp"
#include "duckdb/common/value_operations/value_operations.hpp"
#include "duckdb/planner/expression/bound_comparison_expression.hpp"
#include "duckdb/planner/expression/bound_constant_expression.hpp"
#include "duckdb/planner/expression/bound_function_expression.hpp"
#include "duckdb/planner/expression/bound_reference_expression.hpp"
#include "duckdb/planner/filter/expression_filter.hpp"
#include "duckdb/planner/filter/tablefilter_internal_functions.hpp"

namespace duckdb {

DynamicFilterData::DynamicFilterData(ExpressionType comparison_type_p, Value constant_p)
    : comparison_type(comparison_type_p), constant(std::move(constant_p)) {
}

bool DynamicFilterData::CompareValue(ExpressionType comparison_type, const Value &constant, const Value &value) {
	switch (comparison_type) {
	case ExpressionType::COMPARE_EQUAL:
		return ValueOperations::Equals(value, constant);
	case ExpressionType::COMPARE_NOTEQUAL:
		return ValueOperations::NotEquals(value, constant);
	case ExpressionType::COMPARE_GREATERTHAN:
		return ValueOperations::GreaterThan(value, constant);
	case ExpressionType::COMPARE_GREATERTHANOREQUALTO:
		return ValueOperations::GreaterThanEquals(value, constant);
	case ExpressionType::COMPARE_LESSTHAN:
		return ValueOperations::LessThan(value, constant);
	case ExpressionType::COMPARE_LESSTHANOREQUALTO:
		return ValueOperations::LessThanEquals(value, constant);
	default:
		throw InternalException("unknown comparison type for DynamicFilter: " + EnumUtil::ToString(comparison_type));
	}
}

FilterPropagateResult DynamicFilterData::CheckStatistics(BaseStatistics &stats, ExpressionType comparison_type,
                                                         const Value &constant) {
	auto col_ref = make_uniq<BoundReferenceExpression>(stats.GetType(), 0);
	auto bound_constant = make_uniq<BoundConstantExpression>(constant);
	auto expr = make_uniq<BoundComparisonExpression>(comparison_type, std::move(col_ref), std::move(bound_constant));
	return ExpressionFilter::CheckExpressionStatistics(*expr, stats);
}

DynamicFilter::DynamicFilter() : TableFilter(TableFilterType::DYNAMIC_FILTER) {
}

DynamicFilter::DynamicFilter(shared_ptr<DynamicFilterData> filter_data_p)
    : TableFilter(TableFilterType::DYNAMIC_FILTER), filter_data(std::move(filter_data_p)) {
}

FilterPropagateResult DynamicFilter::CheckStatistics(BaseStatistics &stats) const {
	TableFilter::ThrowDeprecated("DynamicFilter");
}

string DynamicFilter::ToString(const string &column_name) const {
	TableFilter::ThrowDeprecated("DynamicFilter");
}

unique_ptr<Expression> DynamicFilter::ToExpression(const Expression &column) const {
	auto func = DynamicFilterScalarFun::GetFunction(column.return_type);
	auto bind_data = make_uniq<DynamicFilterFunctionData>(filter_data);
	vector<unique_ptr<Expression>> args;
	args.push_back(column.Copy());
	return make_uniq<BoundFunctionExpression>(LogicalType::BOOLEAN, std::move(func), std::move(args),
	                                          std::move(bind_data));
}

bool DynamicFilter::Equals(const TableFilter &other_p) const {
	TableFilter::ThrowDeprecated("DynamicFilter");
}

unique_ptr<TableFilter> DynamicFilter::Copy() const {
	TableFilter::ThrowDeprecated("DynamicFilter");
}

void DynamicFilterData::SetValue(Value val) {
	if (val.IsNull()) {
		return;
	}
	lock_guard<mutex> l(lock);
	constant = std::move(val);
	initialized = true;
}

void DynamicFilterData::Reset() {
	lock_guard<mutex> l(lock);
	initialized = false;
}

} // namespace duckdb
