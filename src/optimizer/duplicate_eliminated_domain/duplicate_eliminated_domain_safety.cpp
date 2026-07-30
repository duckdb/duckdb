//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/optimizer/duplicate_eliminated_domain/duplicate_eliminated_domain_safety.cpp
//
//
//===----------------------------------------------------------------------===//

#include "duckdb/optimizer/duplicate_eliminated_domain/duplicate_eliminated_domain_safety.hpp"

#include "duckdb/function/lambda_functions.hpp"
#include "duckdb/function/window_function.hpp"
#include "duckdb/planner/expression/bound_aggregate_expression.hpp"
#include "duckdb/planner/expression/bound_cast_expression.hpp"
#include "duckdb/planner/expression/bound_function_expression.hpp"
#include "duckdb/planner/expression/bound_window_expression.hpp"
#include "duckdb/planner/expression_iterator.hpp"
#include "duckdb/planner/filter/expression_filter.hpp"
#include "duckdb/planner/filter/table_filter_functions.hpp"
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

template <class PREDICATE>
static bool EmbeddedExpressionsAreSafe(const Expression &expr, const PREDICATE &predicate) {
	if (expr.GetExpressionClass() == ExpressionClass::BOUND_FUNCTION) {
		auto &function = expr.Cast<BoundFunctionExpression>();
		if (function.Function().HasBindLambdaCallback() && function.BindInfo()) {
			auto lambda_expression = function.BindInfo()->Cast<LambdaFunctionData>().GetLambdaExpression();
			if (lambda_expression &&
			    (!predicate(*lambda_expression) || !EmbeddedExpressionsAreSafe(*lambda_expression, predicate))) {
				return false;
			}
		}
		if (function.Function().GetName() == OptionalFilterScalarFun::NAME && function.BindInfo()) {
			auto &data = function.BindInfo()->Cast<OptionalFilterFunctionData>();
			if (data.child_filter_expr && (!predicate(*data.child_filter_expr) ||
			                               !EmbeddedExpressionsAreSafe(*data.child_filter_expr, predicate))) {
				return false;
			}
		}
		if (function.Function().GetName() == SelectivityOptionalFilterScalarFun::NAME && function.BindInfo()) {
			auto &data = function.BindInfo()->Cast<SelectivityOptionalFilterFunctionData>();
			if (data.child_filter_expr && (!predicate(*data.child_filter_expr) ||
			                               !EmbeddedExpressionsAreSafe(*data.child_filter_expr, predicate))) {
				return false;
			}
		}
	}
	bool safe = true;
	ExpressionIterator::EnumerateChildren(
	    expr, [&](const Expression &child) { safe &= EmbeddedExpressionsAreSafe(child, predicate); });
	return safe;
}

static bool ExpressionsCanEvaluateAdditionalGroups(const LogicalOperator &op) {
	bool safe = true;
	LogicalOperatorVisitor::EnumerateExpressions(op, [&](const unique_ptr<Expression> *expression) {
		if (!expression || !*expression) {
			return;
		}
		auto &expr = **expression;
		auto predicate = [](const Expression &candidate) {
			return !CanThrowForAdditionalGroups(candidate) && !candidate.IsVolatile() && !candidate.HasSubquery();
		};
		if (!predicate(expr) || !EmbeddedExpressionsAreSafe(expr, predicate)) {
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
		auto predicate = [](const Expression &candidate) {
			return !candidate.CanThrow() && !candidate.IsVolatile() && !candidate.HasSubquery();
		};
		if (!predicate(**expression) || !EmbeddedExpressionsAreSafe(**expression, predicate)) {
			safe = false;
		}
	});
	return safe;
}

template <class PREDICATE>
static bool TableFiltersAreSafe(const LogicalGet &get, const PREDICATE &predicate) {
	for (const auto &entry : get.table_filters) {
		auto &filter = ExpressionFilter::GetExpressionFilter(entry.Filter(),
		                                                     "DuplicateEliminatedDomainSafety::TableFiltersAreSafe");
		if (!predicate(*filter.expr) || !EmbeddedExpressionsAreSafe(*filter.expr, predicate)) {
			return false;
		}
	}
	return true;
}

static bool CanEvaluateAdditionalGroupsInternal(const LogicalOperator &op, TableIndex domain_cte_index);

static bool ChildrenCanEvaluateAdditionalGroups(const LogicalOperator &op, TableIndex domain_cte_index) {
	for (auto &child : op.children) {
		if (!CanEvaluateAdditionalGroupsInternal(*child, domain_cte_index)) {
			return false;
		}
	}
	return true;
}

static bool CanEvaluateAdditionalGroupsInternal(const LogicalOperator &op, TableIndex domain_cte_index) {
	if (op.type == LogicalOperatorType::LOGICAL_CTE_REF && op.Cast<LogicalCTERef>().cte_index == domain_cte_index) {
		return true;
	}
	if (!ExpressionsCanEvaluateAdditionalGroups(op)) {
		return false;
	}
	switch (op.type) {
	case LogicalOperatorType::LOGICAL_GET: {
		// A table-in/out function can be invoked once per domain group.
		auto &get = op.Cast<LogicalGet>();
		return !get.HasTableInOutInput() && TableFiltersAreSafe(get, [](const Expression &expr) {
			return !CanThrowForAdditionalGroups(expr) && !expr.IsVolatile() && !expr.HasSubquery();
		});
	}
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
	case LogicalOperatorType::LOGICAL_ANY_JOIN:
	case LogicalOperatorType::LOGICAL_ASOF_JOIN:
	case LogicalOperatorType::LOGICAL_COMPARISON_JOIN:
		if (op.Cast<LogicalJoin>().join_type == JoinType::SINGLE) {
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

bool DuplicateEliminatedDomainSafety::CanEvaluateAdditionalGroups(const LogicalOperator &op,
                                                                  TableIndex domain_cte_index) {
	return !op.HasSideEffects() && CanEvaluateAdditionalGroupsInternal(op, domain_cte_index);
}

static bool IsCopyableScan(const LogicalGet &get) {
	if (get.HasTableInOutInput() || !get.function.get_bind_info) {
		return false;
	}
	auto bind_info = get.function.get_bind_info(get.bind_data.get());
	return bind_info.type == ScanType::TABLE || bind_info.type == ScanType::PARQUET;
}

static bool CanDuplicateSourceInternal(const LogicalOperator &op) {
	if (!ExpressionsCanBeDuplicated(op)) {
		return false;
	}
	switch (op.type) {
	case LogicalOperatorType::LOGICAL_GET: {
		auto &get = op.Cast<LogicalGet>();
		return IsCopyableScan(get) && TableFiltersAreSafe(get, [](const Expression &expr) {
			       return !expr.CanThrow() && !expr.IsVolatile() && !expr.HasSubquery();
		       });
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
