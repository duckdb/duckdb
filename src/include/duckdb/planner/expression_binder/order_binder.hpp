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

//! The ORDER binder is responsible for binding an expression within the ORDER BY clause of a SQL statement
class OrderBinder {
public:
	OrderBinder(vector<Binder *> binders, idx_t projection_index, case_insensitive_map_t<idx_t> &alias_map,
	            expression_map_t<idx_t> &projection_map, idx_t max_count);
	OrderBinder(vector<Binder *> binders, idx_t projection_index, SelectNode &node,
	            case_insensitive_map_t<idx_t> &alias_map, expression_map_t<idx_t> &projection_map);

public:
	unique_ptr<Expression> Bind(unique_ptr<ParsedExpression> expr);

	idx_t MaxCount() {
		return max_count;
	}

	unique_ptr<Expression> CreateExtraReference(unique_ptr<ParsedExpression> expr);

private:
	unique_ptr<Expression> CreateProjectionReference(ParsedExpression &expr, idx_t index);
	unique_ptr<Expression> BindConstant(ParsedExpression &expr, const Value &val);

private:
	vector<Binder *> binders;
	idx_t projection_index;
	idx_t max_count;
	vector<unique_ptr<ParsedExpression>> *extra_list;
	case_insensitive_map_t<idx_t> &alias_map;
	expression_map_t<idx_t> &projection_map;
};

} // namespace duckdb
