//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/optimizer/duplicate_eliminated_domain/duplicate_eliminated_domain_safety.cpp
//
//
//===----------------------------------------------------------------------===//

#include "duckdb/optimizer/duplicate_eliminated_domain/duplicate_eliminated_domain_safety.hpp"

#include "duckdb/function/window_function.hpp"
#include "duckdb/planner/expression/bound_aggregate_expression.hpp"
#include "duckdb/planner/expression/bound_cast_expression.hpp"
#include "duckdb/planner/expression/bound_function_expression.hpp"
#include "duckdb/planner/expression/bound_window_expression.hpp"
#include "duckdb/planner/expression_iterator.hpp"
#include "duckdb/planner/filter/expression_filter.hpp"
#include "duckdb/planner/logical_operator_visitor.hpp"
#include "duckdb/planner/operator/list.hpp"

namespace duckdb {

enum class EvaluationMode : uint8_t { ADDITIONAL_GROUPS, FACTOR_SOURCE, DUPLICATE_SOURCE };

static bool IsFloatingPointArithmetic(const BoundFunctionExpression &function) {
	auto return_type = function.GetReturnType().id();
	auto &name = function.Function().GetName();
	return (return_type == LogicalTypeId::FLOAT || return_type == LogicalTypeId::DOUBLE) &&
	       (name == "+" || name == "-" || name == "*" || name == "/");
}

static bool ExpressionIsSafe(const Expression &expr, EvaluationMode mode) {
	if (expr.IsVolatile() || expr.HasSubquery()) {
		return false;
	}
	switch (expr.GetExpressionClass()) {
	case ExpressionClass::BOUND_AGGREGATE: {
		auto &aggregate = expr.Cast<BoundAggregateExpression>();
		if (aggregate.BindInfo()) {
			return false;
		}
		if (mode != EvaluationMode::FACTOR_SOURCE &&
		    aggregate.Function().GetErrorMode() == FunctionErrors::CAN_THROW_RUNTIME_ERROR) {
			return false;
		}
		break;
	}
	case ExpressionClass::BOUND_FUNCTION: {
		auto &function = expr.Cast<BoundFunctionExpression>();
		if (BoundCastExpression::IsCast(function)) {
			if (mode != EvaluationMode::FACTOR_SOURCE &&
			    BoundCastExpression::CastCanThrow(BoundCastExpression::SourceType(function),
			                                      BoundCastExpression::TargetType(function),
			                                      BoundCastExpression::IsTryCast(function))) {
				return false;
			}
		} else {
			if (function.BindInfo()) {
				return false;
			}
			if (mode != EvaluationMode::FACTOR_SOURCE &&
			    function.Function().GetErrorMode() == FunctionErrors::CAN_THROW_RUNTIME_ERROR &&
			    !IsFloatingPointArithmetic(function)) {
				return false;
			}
		}
		break;
	}
	case ExpressionClass::BOUND_WINDOW: {
		auto &window = expr.Cast<BoundWindowExpression>();
		if (window.BindInfo()) {
			return false;
		}
		if (mode != EvaluationMode::FACTOR_SOURCE &&
		    ((window.AggregateFunction() &&
		      window.AggregateFunction()->GetErrorMode() == FunctionErrors::CAN_THROW_RUNTIME_ERROR) ||
		     (window.WindowFunction() &&
		      window.WindowFunction()->GetErrorMode() == FunctionErrors::CAN_THROW_RUNTIME_ERROR))) {
			return false;
		}
		break;
	}
	default:
		break;
	}

	bool safe = true;
	ExpressionIterator::EnumerateChildren(expr,
	                                      [&](const Expression &child) { safe &= ExpressionIsSafe(child, mode); });
	return safe;
}

static bool OperatorExpressionsAreSafe(const LogicalOperator &op, EvaluationMode mode) {
	bool safe = true;
	LogicalOperatorVisitor::EnumerateExpressions(op, [&](const unique_ptr<Expression> *expression) {
		if (expression && *expression) {
			safe &= ExpressionIsSafe(**expression, mode);
		}
	});
	return safe;
}

static bool TableFiltersAreSafe(const LogicalGet &get, EvaluationMode mode) {
	for (const auto &entry : get.table_filters) {
		auto &filter = ExpressionFilter::GetExpressionFilter(entry.Filter(), "DuplicateEliminatedDomainSafety");
		if (!ExpressionIsSafe(*filter.expr, mode)) {
			return false;
		}
	}
	return true;
}

static bool IsSupportedScan(const LogicalGet &get) {
	if (get.HasTableInOutInput() || !get.function.get_bind_info) {
		return false;
	}
	auto bind_info = get.function.get_bind_info(get.bind_data.get());
	return bind_info.type == ScanType::TABLE || bind_info.type == ScanType::PARQUET;
}

static bool CanEvaluateAdditionalGroupsInternal(const LogicalOperator &op, TableIndex domain_cte_index) {
	if (op.type == LogicalOperatorType::LOGICAL_CTE_REF && op.Cast<LogicalCTERef>().cte_index == domain_cte_index) {
		return true;
	}
	if (!OperatorExpressionsAreSafe(op, EvaluationMode::ADDITIONAL_GROUPS)) {
		return false;
	}
	switch (op.type) {
	case LogicalOperatorType::LOGICAL_GET: {
		auto &get = op.Cast<LogicalGet>();
		if (!IsSupportedScan(get) || !TableFiltersAreSafe(get, EvaluationMode::ADDITIONAL_GROUPS)) {
			return false;
		}
		break;
	}
	case LogicalOperatorType::LOGICAL_COMPARISON_JOIN:
	case LogicalOperatorType::LOGICAL_ANY_JOIN:
	case LogicalOperatorType::LOGICAL_ASOF_JOIN:
		if (op.Cast<LogicalJoin>().join_type == JoinType::SINGLE) {
			return false;
		}
		break;
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
	case LogicalOperatorType::LOGICAL_CROSS_PRODUCT:
	case LogicalOperatorType::LOGICAL_CHUNK_GET:
	case LogicalOperatorType::LOGICAL_DUMMY_SCAN:
	case LogicalOperatorType::LOGICAL_EMPTY_RESULT:
	case LogicalOperatorType::LOGICAL_CTE_REF:
		break;
	default:
		return false;
	}
	for (auto &child : op.children) {
		if (!CanEvaluateAdditionalGroupsInternal(*child, domain_cte_index)) {
			return false;
		}
	}
	return true;
}

bool DuplicateEliminatedDomainSafety::CanEvaluateAdditionalGroups(const LogicalOperator &op,
                                                                  TableIndex domain_cte_index) {
	return !op.HasSideEffects() && CanEvaluateAdditionalGroupsInternal(op, domain_cte_index);
}

static bool CanFactorSourceInternal(const LogicalOperator &op) {
	if (!OperatorExpressionsAreSafe(op, EvaluationMode::FACTOR_SOURCE)) {
		return false;
	}
	switch (op.type) {
	case LogicalOperatorType::LOGICAL_GET: {
		auto &get = op.Cast<LogicalGet>();
		return IsSupportedScan(get) && TableFiltersAreSafe(get, EvaluationMode::FACTOR_SOURCE);
	}
	case LogicalOperatorType::LOGICAL_FILTER:
	case LogicalOperatorType::LOGICAL_PROJECTION:
	case LogicalOperatorType::LOGICAL_AGGREGATE_AND_GROUP_BY:
		if (op.children.size() != 1) {
			return false;
		}
		break;
	case LogicalOperatorType::LOGICAL_COMPARISON_JOIN: {
		auto &join = op.Cast<LogicalComparisonJoin>();
		if (op.children.size() != 2 || join.HasProjectionMap() ||
		    (join.join_type != JoinType::INNER && join.join_type != JoinType::SEMI)) {
			return false;
		}
		break;
	}
	default:
		return false;
	}
	for (auto &child : op.children) {
		if (!CanFactorSourceInternal(*child)) {
			return false;
		}
	}
	return true;
}

bool DuplicateEliminatedDomainSafety::CanFactorSource(const LogicalOperator &op) {
	return !op.HasSideEffects() && CanFactorSourceInternal(op);
}

static bool CanDuplicateSourceInternal(const LogicalOperator &op) {
	if (!OperatorExpressionsAreSafe(op, EvaluationMode::DUPLICATE_SOURCE)) {
		return false;
	}
	switch (op.type) {
	case LogicalOperatorType::LOGICAL_GET: {
		auto &get = op.Cast<LogicalGet>();
		return IsSupportedScan(get) && TableFiltersAreSafe(get, EvaluationMode::DUPLICATE_SOURCE);
	}
	case LogicalOperatorType::LOGICAL_FILTER:
	case LogicalOperatorType::LOGICAL_PROJECTION:
		return op.children.size() == 1 && CanDuplicateSourceInternal(*op.children[0]);
	default:
		return false;
	}
}

bool DuplicateEliminatedDomainSafety::CanDuplicateSource(const LogicalOperator &op) {
	return !op.HasSideEffects() && CanDuplicateSourceInternal(op);
}

} // namespace duckdb
