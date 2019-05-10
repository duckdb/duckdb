//===----------------------------------------------------------------------===//
//                         DuckDB
//
// parser/expression_map.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "common/unordered_map.hpp"
#include "common/unordered_set.hpp"
#include "parser/base_expression.hpp"

namespace duckdb {

struct ExpressionHashFunction {
	uint64_t operator()(const BaseExpression *const &expr) const {
		return (uint64_t)expr->Hash();
	}
};

struct ExpressionEquality {
	bool operator()(const BaseExpression *const &a, const BaseExpression *const &b) const {
		return a->Equals(b);
	}
};

template <typename T>
using expression_map_t = unordered_map<BaseExpression *, T, ExpressionHashFunction, ExpressionEquality>;

using expression_set_t = unordered_set<BaseExpression *, ExpressionHashFunction, ExpressionEquality>;

} // namespace duckdb
