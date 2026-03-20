//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/planner/expression/bound_window_function.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/planner/expression.hpp"
#include "duckdb/planner/bound_result_modifier.hpp"
#include "duckdb/function/window_function.hpp"
#include "duckdb/function/aggregate_state.hpp"

namespace duckdb {

class BoundWindowFunction : public Expression {
public:
	static constexpr const ExpressionClass TYPE = ExpressionClass::BOUND_WINDOW_FUNCTION;

public:
	BoundWindowFunction(WindowFunction function, vector<unique_ptr<Expression>> children,
	                    unique_ptr<FunctionData> bind_info);

	//! The bound function expression
	WindowFunction function;
	//! List of arguments to the function
	vector<unique_ptr<Expression>> children;
	//! The bound function data (if any)
	unique_ptr<FunctionData> bind_info;
	//! The aggregate type (distinct or non-distinct)
	AggregateType aggr_type = AggregateType::NON_DISTINCT;

	//! Filter for this window
	unique_ptr<Expression> filter;
	//! The order by expression for this window - if any
	unique_ptr<BoundOrderModifier> order_bys;
	//! The order by arguments for this window - if any
	unique_ptr<BoundOrderModifier> arg_order_bys;

public:
	bool IsDistinct() const {
		return aggr_type == AggregateType::DISTINCT;
	}

	bool IsWindow() const override {
		return true;
	}
	bool IsFoldable() const override {
		return false;
	}

	string ToString() const override;

	unique_ptr<Expression> Copy() const override;
};

} // namespace duckdb
