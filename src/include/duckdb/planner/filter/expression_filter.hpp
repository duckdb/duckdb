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
#include "duckdb/planner/expression/bound_comparison_expression.hpp"
#include "duckdb/planner/expression/bound_constant_expression.hpp"
#include "duckdb/planner/expression/bound_conjunction_expression.hpp"

namespace duckdb {
class ExpressionExecutor;
struct DynamicFilterData;

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

	//! Convert a legacy/deserialized TableFilter to an ExpressionFilter using a BoundReferenceExpression(0)
	//! as the column placeholder. The actual column type must be provided by the caller.
	static unique_ptr<ExpressionFilter> FromTableFilter(const TableFilter &filter, const LogicalType &col_type);
	//! Access a runtime ExpressionFilter, failing fast if a legacy filter somehow reached an active code path.
	static const ExpressionFilter &GetExpressionFilter(const TableFilter &filter, const char *context);
	static ExpressionFilter &GetExpressionFilter(TableFilter &filter, const char *context);
	//! Build a COMPARE_IN expression over a single-column filter subject.
	static unique_ptr<Expression> CreateInExpression(unique_ptr<Expression> column, vector<Value> values);
	//! Build an IS NULL/IS NOT NULL expression over a single-column filter subject.
	static unique_ptr<Expression> CreateNullCheckExpression(unique_ptr<Expression> column,
	                                                        ExpressionType expression_type);

	//! Enhanced CheckStatistics that recognizes standard expression patterns
	static FilterPropagateResult CheckExpressionStatistics(const Expression &expr, BaseStatistics &stats);
	//! Check if an expression tree contains an internal function with the given name
	static bool ContainsInternalFunction(const Expression &expr, const string &func_name);
	//! Check if an expression tree is entirely optional filter semantics
	static bool IsOptionalExpression(const Expression &expr);
	//! Check if the root of an expression tree is an optional filter wrapper
	static bool IsRootOptionalExpression(const Expression &expr);
	//! Check if a table filter tree is entirely optional filter semantics
	static bool IsOptionalFilter(const TableFilter &filter);
	//! Check if the root of a table filter tree is an optional filter wrapper
	static bool IsRootOptionalFilter(const TableFilter &filter);
	//! If this is an optional/selectivity-optional wrapper around a root dynamic filter,
	//! return the shared dynamic filter state.
	static shared_ptr<DynamicFilterData> GetRootOptionalDynamicFilterData(const TableFilter &filter);

	FilterPropagateResult CheckStatistics(BaseStatistics &stats) const;
	string ToString(const string &column_name) const;
	bool Equals(const ExpressionFilter &other) const;
	unique_ptr<ExpressionFilter> Copy() const;
	unique_ptr<Expression> ToExpression(const Expression &column) const override;
	void Serialize(Serializer &serializer) const override;
	static unique_ptr<TableFilter> Deserialize(Deserializer &deserializer);
	static void ReplaceExpressionRecursive(unique_ptr<Expression> &expr, const Expression &column,
	                                       ExpressionType replace_type = ExpressionType::BOUND_REF);

private:
	//! Produce human-readable ToString for internal tablefilter functions
	static string InternalFunctionToString(const BoundFunctionExpression &func_expr, const string &column_name);
	//! Recursively convert expression to friendly string, handling internal functions
	static string ExpressionToFriendlyString(const Expression &expression, const string &column_name);
};

} // namespace duckdb
