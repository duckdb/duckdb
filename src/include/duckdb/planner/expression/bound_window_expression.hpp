//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/planner/expression/bound_window_expression.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/parser/expression/window_expression.hpp"
#include "duckdb/function/function.hpp"
#include "duckdb/planner/expression.hpp"
#include "duckdb/planner/bound_result_modifier.hpp"

namespace duckdb {
class AggregateFunction;
class WindowFunction;

class BoundWindowExpression : public Expression {
public:
	static constexpr const ExpressionClass TYPE = ExpressionClass::BOUND_WINDOW;

public:
	BoundWindowExpression(LogicalType return_type, unique_ptr<AggregateFunction> aggregate,
	                      unique_ptr<WindowFunction> window, unique_ptr<FunctionData> bind_info);

	//! The bound aggregate function
	unique_ptr<AggregateFunction> aggregate;
	//! The bound window function
	unique_ptr<WindowFunction> window;
	//! The bound function info
	unique_ptr<FunctionData> bind_info;
	//! The child expressions of the main window function
	vector<unique_ptr<Expression>> children;
	//! The set of expressions to partition by
	vector<unique_ptr<Expression>> partitions;
	//! Statistics belonging to the partitions expressions
	vector<unique_ptr<BaseStatistics>> partitions_stats;
	//! The set of ordering clauses
	vector<BoundOrderByNode> orders;
	//! Expression representing a filter, only used for aggregates
	unique_ptr<Expression> filter_expr;
	//! True to ignore NULL values
	bool ignore_nulls;
	//! Whether or not the aggregate function is distinct, only used for aggregates
	bool distinct;
	//! The window boundaries
	WindowBoundary start = WindowBoundary::INVALID;
	WindowBoundary end = WindowBoundary::INVALID;
	//! The EXCLUDE clause
	WindowExcludeMode exclude_clause = WindowExcludeMode::NO_OTHER;

	unique_ptr<Expression> start_expr;
	unique_ptr<Expression> end_expr;

	//! The set of argument ordering clauses
	//! These are distinct from the frame ordering clauses e.g., the "x" in
	//! FIRST_VALUE(a ORDER BY x) OVER (PARTITION BY p ORDER BY s)
	vector<BoundOrderByNode> arg_orders;

	//! Statistics belonging to the other expressions (start, end, offset, default)
	vector<unique_ptr<BaseStatistics>> expr_stats;

public:
	bool IsWindow() const override {
		return true;
	}
	bool IsFoldable() const override {
		return false;
	}

	string ToString() const override;

	//! The number of ordering clauses the functions share
	static idx_t GetSharedOrders(const vector<BoundOrderByNode> &lhs, const vector<BoundOrderByNode> &rhs);
	idx_t GetSharedOrders(const BoundWindowExpression &other) const;

	bool PartitionsAreEquivalent(const BoundWindowExpression &other) const;
	bool KeysAreCompatible(const BoundWindowExpression &other) const;
	bool Equals(const BaseExpression &other) const override;

	unique_ptr<Expression> Copy() const override;

	void Serialize(Serializer &serializer) const override;
	static unique_ptr<Expression> Deserialize(Deserializer &deserializer);
};
} // namespace duckdb
