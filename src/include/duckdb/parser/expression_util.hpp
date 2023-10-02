//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/parser/expression_util.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/parser/base_expression.hpp"
#include "duckdb/common/vector.hpp"

namespace duckdb {
class ParsedExpression;
class Expression;

class ExpressionUtil {
public:
	//! ListEquals: check if a list of two expressions is equal (order is important)
	static bool ListEquals(const vector<unique_ptr<ParsedExpression>> &a,
	                       const vector<unique_ptr<ParsedExpression>> &b);
	static bool ListEquals(const vector<unique_ptr<Expression>> &a, const vector<unique_ptr<Expression>> &b);
	//! SetEquals: check if two sets of expressions are equal (order is not important)
	static bool SetEquals(const vector<unique_ptr<ParsedExpression>> &a, const vector<unique_ptr<ParsedExpression>> &b);
	static bool SetEquals(const vector<unique_ptr<Expression>> &a, const vector<unique_ptr<Expression>> &b);

private:
	template <class T>
	static bool ExpressionListEquals(const vector<unique_ptr<T>> &a, const vector<unique_ptr<T>> &b);
	template <class T, class EXPRESSION_MAP>
	static bool ExpressionSetEquals(const vector<unique_ptr<T>> &a, const vector<unique_ptr<T>> &b);
};

} // namespace duckdb
