//===----------------------------------------------------------------------===//
//                         DuckDB
//
// planner/expression_iterator.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "common/common.hpp"
#include "planner/expression.hpp"

#include <functional>

namespace duckdb {

class ExpressionIterator {
public:
	static void EnumerateChildren(Expression &expression, std::function<void(Expression &child)> callback);
	static void EnumerateChildren(Expression &expression,
	                              std::function<unique_ptr<Expression>(unique_ptr<Expression> child)> callback);
};

} // namespace duckdb
