//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/planner/expression_sources.hpp
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/optional_ptr.hpp"
#include "duckdb/planner/column_binding.hpp"

namespace duckdb {
class Binder;
class BoundSelectNode;
class Expression;

enum class ExpressionSourceKind : uint8_t { NONE, GROUP, AGGREGATE, WINDOW, UNNEST };

//! A borrowed expression source, valid only while its SELECT is being bound.
struct BoundExpressionSource {
	optional_ptr<const Expression> expression;
	ExpressionSourceKind kind = ExpressionSourceKind::NONE;
};

//! Exposes a SELECT's registered expressions until ownership passes to its logical plan.
class ExpressionSourceScope {
public:
	ExpressionSourceScope(Binder &binder, const BoundSelectNode &node);
	~ExpressionSourceScope();
	ExpressionSourceScope(const ExpressionSourceScope &) = delete;
	ExpressionSourceScope &operator=(const ExpressionSourceScope &) = delete;

	void Release();

private:
	Binder &binder;
	optional_ptr<const BoundSelectNode> node;
};
} // namespace duckdb
