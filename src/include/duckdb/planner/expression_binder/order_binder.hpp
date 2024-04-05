//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/planner/expression_binder/order_binder.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/case_insensitive_map.hpp"
#include "duckdb/parser/expression_map.hpp"
#include "duckdb/parser/parsed_expression.hpp"
#include "duckdb/planner/expression/bound_columnref_expression.hpp"

namespace duckdb {
class Binder;
class Expression;
class SelectNode;
struct SelectBindState;

//! The ORDER binder is responsible for binding an expression within the ORDER BY clause of a SQL statement
class OrderBinder {
public:
	OrderBinder(vector<Binder *> binders, SelectBindState &bind_state);
	OrderBinder(vector<Binder *> binders, SelectNode &node, SelectBindState &bind_state);

public:
	unique_ptr<Expression> Bind(unique_ptr<ParsedExpression> expr);

	bool HasExtraList() const {
		return extra_list;
	}
	const vector<Binder *> &GetBinders() const {
		return binders;
	}

	unique_ptr<Expression> CreateExtraReference(unique_ptr<ParsedExpression> expr);

private:
	unique_ptr<Expression> CreateProjectionReference(ParsedExpression &expr, const idx_t index);
	unique_ptr<Expression> BindConstant(ParsedExpression &expr, const Value &val);

private:
	vector<Binder *> binders;
	optional_ptr<vector<unique_ptr<ParsedExpression>>> extra_list;
	SelectBindState &bind_state;
};

} // namespace duckdb
