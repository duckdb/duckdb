//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/parser/parsed_expression_iterator.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/parser/parsed_expression.hpp"

#include <functional>

namespace duckdb {

class ParsedExpressionIterator {
public:
	static void EnumerateChildren(const ParsedExpression &expression,
	                              std::function<void(const ParsedExpression &child)> callback);
};

} // namespace duckdb
