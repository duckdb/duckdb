//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/optimizer/scalar_fn_pushdown.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once
#include "duckdb/optimizer/type_pushdown.hpp"

namespace duckdb {

/**
 * Collect fn(col) expressions i.e. expressions where a single function (not
 * a function chain) wraps a single bound column. If "col" is used without
 * function application in "plan", record in "analyses.conflicts"
 */
struct ScalarFnCollect final : LogicalOperatorVisitor {
	Analyses &analyses;
	const Projections &projections;

	ScalarFnCollect(Analyses &analyses, const Projections &projections);
	void VisitOperator(LogicalOperator &op) override;
	unique_ptr<Expression> VisitReplace(BoundColumnRefExpression &expr, unique_ptr<Expression> *ptr) override;
	unique_ptr<Expression> VisitReplace(BoundFunctionExpression &expr, unique_ptr<Expression> *ptr) override;
};

/*
 * For "col" in columns collected by ScalarFnCollect, replace fn(col) to "col"
 * if "col" doesn't have conflicting usage. Update return types for bound
 * columns and logical projections referencing this column.
 */
struct ScalarFnReplace final : LogicalOperatorVisitor {
	Analyses &analyses;
	const Projections &projections;

	ScalarFnReplace(Analyses &analyses, const Projections &aliases);
	unique_ptr<Expression> VisitReplace(BoundColumnRefExpression &expr, unique_ptr<Expression> *ptr) override;
	unique_ptr<Expression> VisitReplace(BoundFunctionExpression &expr, unique_ptr<Expression> *ptr) override;
};

class ScalarFnPushdown {
public:
	explicit inline ScalarFnPushdown(ClientContext &context) : context(context) {
	}
	unique_ptr<LogicalOperator> Optimize(unique_ptr<LogicalOperator> op) {
		return PushdownOptimize<ScalarFnCollect, ScalarFnReplace>(context, std::move(op));
	}

private:
	ClientContext &context;
};
} // namespace duckdb
