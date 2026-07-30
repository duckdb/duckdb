//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/optimizer/duplicate_eliminated_domain_safety.cpp
//
//
//===----------------------------------------------------------------------===//

#include "duckdb/optimizer/duplicate_eliminated_domain_safety.hpp"

#include "duckdb/function/window_function.hpp"
#include "duckdb/planner/expression/bound_aggregate_expression.hpp"
#include "duckdb/planner/expression/bound_cast_expression.hpp"
#include "duckdb/planner/expression/bound_function_expression.hpp"
#include "duckdb/planner/expression/bound_window_expression.hpp"
#include "duckdb/planner/expression_iterator.hpp"
#include "duckdb/planner/logical_operator_visitor.hpp"
#include "duckdb/planner/operator/list.hpp"

namespace duckdb {

static bool CanThrowForAdditionalGroups(const Expression &expr) {
	if (expr.GetExpressionClass() == ExpressionClass::BOUND_AGGREGATE &&
	    expr.Cast<BoundAggregateExpression>().Function().GetErrorMode() == FunctionErrors::CAN_THROW_RUNTIME_ERROR) {
		return true;
	}
	if (expr.GetExpressionClass() == ExpressionClass::BOUND_WINDOW) {
		auto &window = expr.Cast<BoundWindowExpression>();
		if ((window.AggregateFunction() &&
		     window.AggregateFunction()->GetErrorMode() == FunctionErrors::CAN_THROW_RUNTIME_ERROR) ||
		    (window.WindowFunction() &&
		     window.WindowFunction()->GetErrorMode() == FunctionErrors::CAN_THROW_RUNTIME_ERROR)) {
			return true;
		}
	}
	if (expr.GetExpressionClass() == ExpressionClass::BOUND_FUNCTION) {
		auto &function = expr.Cast<BoundFunctionExpression>();
		if (BoundCastExpression::IsCast(function)) {
			if (BoundCastExpression::CastCanThrow(BoundCastExpression::SourceType(function),
			                                      BoundCastExpression::TargetType(function),
			                                      BoundCastExpression::IsTryCast(function))) {
				return true;
			}
		} else if (function.Function().GetErrorMode() == FunctionErrors::CAN_THROW_RUNTIME_ERROR) {
			auto return_type = function.GetReturnType().id();
			auto &name = function.Function().GetName();
			bool floating_point_arithmetic =
			    (return_type == LogicalTypeId::FLOAT || return_type == LogicalTypeId::DOUBLE) &&
			    (name == "+" || name == "-" || name == "*" || name == "/");
			if (!floating_point_arithmetic) {
				return true;
			}
		}
	}
	bool child_can_throw = false;
	ExpressionIterator::EnumerateChildren(
	    expr, [&](const Expression &child) { child_can_throw |= CanThrowForAdditionalGroups(child); });
	return child_can_throw;
}

static bool ExpressionsCanEvaluateAdditionalGroups(const LogicalOperator &op) {
	bool safe = true;
	LogicalOperatorVisitor::EnumerateExpressions(op, [&](const unique_ptr<Expression> *expression) {
		if (!expression || !*expression) {
			return;
		}
		auto &expr = **expression;
		if (CanThrowForAdditionalGroups(expr) || expr.IsVolatile() || expr.HasSubquery()) {
			safe = false;
		}
	});
	return safe;
}

static bool ExpressionsCanBeDuplicated(const LogicalOperator &op) {
	bool safe = true;
	LogicalOperatorVisitor::EnumerateExpressions(op, [&](const unique_ptr<Expression> *expression) {
		if (!expression || !*expression) {
			return;
		}
		if ((*expression)->CanThrow() || (*expression)->IsVolatile() || (*expression)->HasSubquery()) {
			safe = false;
		}
	});
	return safe;
}

static bool ChildrenCanEvaluateAdditionalGroups(const LogicalOperator &op, TableIndex domain_cte_index) {
	for (auto &child : op.children) {
		if (!DuplicateEliminatedDomainSafety::CanEvaluateAdditionalGroups(*child, domain_cte_index)) {
			return false;
		}
	}
	return true;
}

bool DuplicateEliminatedDomainSafety::CanEvaluateAdditionalGroups(const LogicalOperator &op,
                                                                  TableIndex domain_cte_index) {
	if (op.type == LogicalOperatorType::LOGICAL_CTE_REF && op.Cast<LogicalCTERef>().cte_index == domain_cte_index) {
		return true;
	}
	if (op.HasSideEffects() || !ExpressionsCanEvaluateAdditionalGroups(op)) {
		return false;
	}
	switch (op.type) {
	case LogicalOperatorType::LOGICAL_GET:
		// A table-in/out function can be invoked once per domain group.
		return op.children.empty() && op.Cast<LogicalGet>().projected_input.empty();
	case LogicalOperatorType::LOGICAL_PROJECTION:
	case LogicalOperatorType::LOGICAL_FILTER:
	case LogicalOperatorType::LOGICAL_AGGREGATE_AND_GROUP_BY:
	case LogicalOperatorType::LOGICAL_WINDOW:
	case LogicalOperatorType::LOGICAL_UNNEST:
	case LogicalOperatorType::LOGICAL_ORDER_BY:
	case LogicalOperatorType::LOGICAL_DISTINCT:
	case LogicalOperatorType::LOGICAL_PIVOT:
	case LogicalOperatorType::LOGICAL_EXPRESSION_GET:
	case LogicalOperatorType::LOGICAL_UNION:
	case LogicalOperatorType::LOGICAL_EXCEPT:
	case LogicalOperatorType::LOGICAL_INTERSECT:
	case LogicalOperatorType::LOGICAL_MATERIALIZED_CTE:
		return ChildrenCanEvaluateAdditionalGroups(op, domain_cte_index);
	case LogicalOperatorType::LOGICAL_COMPARISON_JOIN:
		if (op.Cast<LogicalComparisonJoin>().join_type == JoinType::SINGLE) {
			return false;
		}
		return ChildrenCanEvaluateAdditionalGroups(op, domain_cte_index);
	case LogicalOperatorType::LOGICAL_CROSS_PRODUCT:
		return ChildrenCanEvaluateAdditionalGroups(op, domain_cte_index);
	case LogicalOperatorType::LOGICAL_CHUNK_GET:
	case LogicalOperatorType::LOGICAL_DUMMY_SCAN:
	case LogicalOperatorType::LOGICAL_EMPTY_RESULT:
	case LogicalOperatorType::LOGICAL_CTE_REF:
		return op.children.empty();
	default:
		// Operators without an explicit proof are unsafe for additional groups.
		return false;
	}
}

static bool IsCopyableScan(const LogicalGet &get) {
	if (!get.children.empty() || !get.projected_input.empty() || !get.function.get_bind_info) {
		return false;
	}
	auto bind_info = get.function.get_bind_info(get.bind_data.get());
	return bind_info.type == ScanType::TABLE || bind_info.type == ScanType::PARQUET;
}

bool DuplicateEliminatedDomainSafety::CanDuplicateSource(const LogicalOperator &op) {
	if (op.HasSideEffects() || !ExpressionsCanBeDuplicated(op)) {
		return false;
	}
	switch (op.type) {
	case LogicalOperatorType::LOGICAL_GET:
		return IsCopyableScan(op.Cast<LogicalGet>());
	case LogicalOperatorType::LOGICAL_FILTER:
	case LogicalOperatorType::LOGICAL_PROJECTION:
		return op.children.size() == 1 && CanDuplicateSource(*op.children[0]);
	default:
		return false;
	}
}

} // namespace duckdb
