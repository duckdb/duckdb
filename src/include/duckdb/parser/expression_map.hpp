//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/parser/expression_map.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/unordered_map.hpp"
#include "duckdb/common/unordered_set.hpp"
#include "duckdb/parser/base_expression.hpp"
#include "duckdb/parser/parsed_expression.hpp"
#include "duckdb/planner/expression.hpp"

namespace duckdb {
class Expression;

template <class T>
struct ExpressionHashFunction {
	uint64_t operator()(const reference<T> &expr) const {
		return (uint64_t)expr.get().Hash();
	}
};

template <class T>
struct ExpressionEquality {
	bool operator()(const reference<T> &a, const reference<T> &b) const {
		return a.get().Equals(b.get());
	}
};

template <typename T>
using expression_map_t =
    unordered_map<reference<Expression>, T, ExpressionHashFunction<Expression>, ExpressionEquality<Expression>>;

using expression_set_t =
    unordered_set<reference<Expression>, ExpressionHashFunction<Expression>, ExpressionEquality<Expression>>;

template <typename T>
using parsed_expression_map_t = unordered_map<reference<ParsedExpression>, T, ExpressionHashFunction<ParsedExpression>,
                                              ExpressionEquality<ParsedExpression>>;

using parsed_expression_set_t = unordered_set<reference<ParsedExpression>, ExpressionHashFunction<ParsedExpression>,
                                              ExpressionEquality<ParsedExpression>>;

} // namespace duckdb
