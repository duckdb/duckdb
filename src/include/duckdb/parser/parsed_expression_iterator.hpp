//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/parser/parsed_expression_iterator.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/parser/parsed_expression.hpp"
#include "duckdb/parser/tokens.hpp"

#include <functional>

namespace duckdb {

class ParsedExpressionIterator {
public:
	static void EnumerateChildren(const ParsedExpression &expression,
	                              const std::function<void(const ParsedExpression &child)> &callback);
	static void EnumerateChildren(ParsedExpression &expr, const std::function<void(ParsedExpression &child)> &callback);
	static void EnumerateChildren(ParsedExpression &expr,
	                              const std::function<void(unique_ptr<ParsedExpression> &child)> &callback);

	static void EnumerateTableRefChildren(TableRef &ref,
	                                      const std::function<void(unique_ptr<ParsedExpression> &child)> &callback);
	static void EnumerateQueryNodeChildren(QueryNode &node,
	                                       const std::function<void(unique_ptr<ParsedExpression> &child)> &callback);

	static void EnumerateQueryNodeModifiers(QueryNode &node,
	                                        const std::function<void(unique_ptr<ParsedExpression> &child)> &callback);
};

} // namespace duckdb
