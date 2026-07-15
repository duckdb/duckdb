//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/planner/subquery/recursive_dependent_join_planner.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/enums/join_type.hpp"
#include "duckdb/common/unordered_set.hpp"
#include "duckdb/optimizer/column_binding_replacer.hpp"
#include "duckdb/planner/logical_operator.hpp"
#include "duckdb/planner/logical_operator_visitor.hpp"

namespace duckdb {

class Binder;
class JoinSide;
class LogicalJoin;

/*
 * Recursively plan subqueries and flatten dependent joins from outermost to innermost (like peeling an onion).
 */
class RecursiveDependentJoinPlanner : public LogicalOperatorVisitor {
public:
	static void Plan(Binder &binder, unique_ptr<LogicalOperator> &op);
	static void PlanJoinConditionSubqueries(Binder &binder, unique_ptr<LogicalOperator> &op);
	static bool CanRewritePairDependentJoinCondition(LogicalOperator &op);

private:
	explicit RecursiveDependentJoinPlanner(Binder &binder) : binder(binder) {
	}
	static bool TryRewritePairDependentJoinCondition(Binder &binder, unique_ptr<LogicalOperator> &op,
	                                                 vector<ReplacementBinding> &replacements);
	vector<ReplacementBinding> PlanOperator(unique_ptr<LogicalOperator> &op);
	void PlanJoinChildFilters(LogicalOperator &op);
	void PlanJoinExpressions(LogicalOperator &op);
	void PlanJoinSubqueries(LogicalJoin &join, unique_ptr<Expression> &expr, JoinSide uncorrelated_side);
	unique_ptr<Expression> VisitReplace(BoundSubqueryExpression &expr, unique_ptr<Expression> *expr_ptr) override;

	unique_ptr<LogicalOperator> root;
	Binder &binder;
};
} // namespace duckdb
