//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/planner/expression_binder/order_binder.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/unordered_map.hpp"
#include "duckdb/parser/expression_map.hpp"
#include "duckdb/parser/parsed_expression.hpp"
#include "duckdb/planner/expression/bound_columnref_expression.hpp"

namespace duckdb {
class Expression;
class SelectNode;

//! The ORDER binder is responsible for binding an expression within the ORDER BY clause of a SQL statement
class OrderBinder {
public:
	OrderBinder(idx_t projection_index, SelectNode &node, unordered_map<string, idx_t> &alias_map,
	            expression_map_t<idx_t> &projection_map, vector<unique_ptr<ParsedExpression>> &extra_select_list);

	unique_ptr<Expression> Bind(unique_ptr<ParsedExpression> expr);
	void RemapIndex(BoundColumnRefExpression &expr, idx_t index);

private:
	unique_ptr<Expression> CreateProjectionReference(ParsedExpression &expr, idx_t index);

	idx_t projection_index;
	SelectNode &node;
	unordered_map<string, idx_t> &alias_map;
	expression_map_t<idx_t> &projection_map;
	vector<unique_ptr<ParsedExpression>> &extra_select_list;
};

} // namespace duckdb
