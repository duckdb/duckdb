//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/planner/expression_iterator.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/common.hpp"
#include "duckdb/planner/expression.hpp"

#include <functional>

namespace duckdb {
class BoundQueryNode;
class BoundTableRef;

class ExpressionIterator {
public:
	static void EnumerateChildren(const Expression &expression, std::function<void(const Expression &child)> callback);
	static void EnumerateChildren(Expression &expression, std::function<void(Expression &child)> callback);
	static void EnumerateChildren(Expression &expression,
	                              std::function<unique_ptr<Expression>(unique_ptr<Expression> child)> callback);

	static void EnumerateExpression(unique_ptr<Expression> &expr, std::function<void(Expression &child)> callback);

	static void EnumerateTableRefChildren(BoundTableRef &ref, std::function<void(Expression &child)> callback);
	static void EnumerateQueryNodeChildren(BoundQueryNode &node, std::function<void(Expression &child)> callback);
};

} // namespace duckdb
