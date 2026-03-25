//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/planner/filter/expression_filter.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/planner/table_filter.hpp"
#include "duckdb/planner/expression.hpp"

namespace duckdb {
class ExpressionExecutor;
class ConstantFilter;
class IsNullFilter;
class IsNotNullFilter;
class InFilter;
class ConjunctionAndFilter;
class ConjunctionOrFilter;
class StructFilter;

class BoundFunctionExpression;

class ExpressionFilter : public TableFilter {
public:
	static constexpr const TableFilterType TYPE = TableFilterType::EXPRESSION_FILTER;

public:
	explicit ExpressionFilter(unique_ptr<Expression> expr);

	//! The expression to evaluate
	unique_ptr<Expression> expr;

public:
	bool EvaluateWithConstant(ClientContext &context, const Value &val) const;
	bool EvaluateWithConstant(ExpressionExecutor &executor, const Value &val) const;

	//! Convert any TableFilter to an ExpressionFilter using a BoundReferenceExpression(0) as column placeholder
	//! col_type should be specified for filters that don't contain constants (IsNull, IsNotNull)
	static unique_ptr<ExpressionFilter> FromTableFilter(const TableFilter &filter,
	                                                    const LogicalType &col_type = LogicalType::ANY);

	//! Enhanced CheckStatistics that recognizes standard expression patterns
	static FilterPropagateResult CheckExpressionStatistics(const Expression &expr, BaseStatistics &stats);

	FilterPropagateResult CheckStatistics(BaseStatistics &stats) const override;
	string ToString(const string &column_name) const override;
	bool Equals(const TableFilter &other) const override;
	unique_ptr<TableFilter> Copy() const override;
	unique_ptr<Expression> ToExpression(const Expression &column) const override;
	void Serialize(Serializer &serializer) const override;
	static unique_ptr<TableFilter> Deserialize(Deserializer &deserializer);
	static void ReplaceExpressionRecursive(unique_ptr<Expression> &expr, const Expression &column,
	                                       ExpressionType replace_type = ExpressionType::BOUND_REF);
	//! Check if an expression tree contains an internal function with the given name
	static bool ContainsInternalFunction(const Expression &expr, const string &func_name);
	//! Check if an expression tree is entirely optional filter semantics
	static bool IsOptionalExpression(const Expression &expr);
	//! Check if the root of an expression tree is an optional filter wrapper
	static bool IsRootOptionalExpression(const Expression &expr);
	//! Check if a table filter is an optional filter, including expression-backed optional filters
	static bool IsOptionalFilter(const TableFilter &filter);
	//! Check if a table filter root is optional, matching legacy OPTIONAL_FILTER behavior
	static bool IsRootOptionalFilter(const TableFilter &filter);

private:
	//! Produce human-readable ToString for internal tablefilter functions
	static string InternalFunctionToString(const BoundFunctionExpression &func_expr, const string &column_name);
	//! Recursively convert expression to friendly string, handling internal functions
	static string ExpressionToFriendlyString(const Expression &expression, const string &column_name);
};

} // namespace duckdb
