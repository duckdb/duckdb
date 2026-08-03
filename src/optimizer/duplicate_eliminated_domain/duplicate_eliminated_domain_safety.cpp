//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/optimizer/duplicate_eliminated_domain/duplicate_eliminated_domain_safety.cpp
//
//
//===----------------------------------------------------------------------===//

#include "duckdb/optimizer/duplicate_eliminated_domain/duplicate_eliminated_domain_safety.hpp"
#include "duckdb/optimizer/duplicate_eliminated_domain/duplicate_eliminated_domain_cte_registry.hpp"

#include "duckdb/execution/expression_executor.hpp"
#include "duckdb/common/operator/subtract.hpp"
#include "duckdb/function/lambda_functions.hpp"
#include "duckdb/function/window_function.hpp"
#include "duckdb/planner/expression/bound_aggregate_expression.hpp"
#include "duckdb/planner/expression/bound_cast_expression.hpp"
#include "duckdb/planner/expression/bound_constant_expression.hpp"
#include "duckdb/planner/expression/bound_function_expression.hpp"
#include "duckdb/planner/expression/bound_window_expression.hpp"
#include "duckdb/planner/expression_iterator.hpp"
#include "duckdb/planner/filter/expression_filter.hpp"
#include "duckdb/planner/logical_operator_repeatability.hpp"
#include "duckdb/planner/logical_operator_visitor.hpp"
#include "duckdb/planner/operator/list.hpp"

#include "duckdb/planner/subquery/duplicate_eliminated_domain_properties.hpp"

namespace duckdb {

struct BigIntRange {
	int64_t minimum;
	int64_t maximum;
};

template <class FUNCTION>
static bool IsSystemFunction(const FUNCTION &function) {
	return function.GetCatalogName() == "system" && function.GetSchemaName() == "main";
}

static bool IsSystemFunction(const BoundScalarFunction &function, const char *name,
                             const vector<LogicalType> &arguments, const LogicalType &return_type) {
	return IsSystemFunction(function) && function.GetName() == name && function.GetArguments() == arguments &&
	       function.GetReturnType() == return_type;
}

static bool IsSystemFloatingPointArithmetic(const BoundScalarFunction &function) {
	for (auto &type : {LogicalType::FLOAT, LogicalType::DOUBLE}) {
		for (auto name : {"+", "-", "*", "/"}) {
			if (IsSystemFunction(function, name, {type, type}, type)) {
				return true;
			}
		}
	}
	return false;
}

static optional<BigIntRange> TryGetBigIntRange(ClientContext &context, const Expression &expr) {
	if (expr.GetReturnType() != LogicalType::BIGINT) {
		return nullopt;
	}
	if (expr.IsFoldable()) {
		Value value;
		if (!ExpressionExecutor::TryEvaluateScalar(context, expr, value) || value.IsNull()) {
			return nullopt;
		}
		auto constant = value.GetValue<int64_t>();
		return BigIntRange {constant, constant};
	}
	if (expr.GetExpressionClass() == ExpressionClass::BOUND_CONSTANT) {
		auto &value = expr.Cast<BoundConstantExpression>().GetValue();
		if (value.IsNull()) {
			return nullopt;
		}
		auto constant = value.GetValue<int64_t>();
		return BigIntRange {constant, constant};
	}
	if (expr.GetExpressionClass() != ExpressionClass::BOUND_FUNCTION) {
		return nullopt;
	}
	auto &function = expr.Cast<BoundFunctionExpression>();
	if (IsSystemFunction(function.Function(), "length", {LogicalType::VARCHAR}, LogicalType::BIGINT) &&
	    function.GetChildren().size() == 1) {
		return BigIntRange {0, NumericLimits<int64_t>::Maximum()};
	}
	if (!IsSystemFunction(function.Function(), "-", {LogicalType::BIGINT, LogicalType::BIGINT}, LogicalType::BIGINT) ||
	    function.GetChildren().size() != 2) {
		return nullopt;
	}
	auto left = TryGetBigIntRange(context, *function.GetChildren()[0]);
	auto right = TryGetBigIntRange(context, *function.GetChildren()[1]);
	if (!left || !right) {
		return nullopt;
	}
	BigIntRange result;
	if (!TrySubtractOperator::Operation(left->minimum, right->maximum, result.minimum) ||
	    !TrySubtractOperator::Operation(left->maximum, right->minimum, result.maximum)) {
		return nullopt;
	}
	return result;
}

static bool CanEvaluateFoldableExpression(ClientContext &context, const Expression &expr) {
	if (!expr.IsFoldable()) {
		return false;
	}
	Value result;
	return ExpressionExecutor::TryEvaluateScalar(context, expr, result);
}

static bool ExpressionIsSafe(ClientContext &context, const Expression &expr) {
	if (expr.IsVolatile() || expr.HasSubquery()) {
		return false;
	}
	switch (expr.GetExpressionClass()) {
	case ExpressionClass::BOUND_AGGREGATE: {
		auto &aggregate = expr.Cast<BoundAggregateExpression>();
		if (aggregate.Function().GetErrorMode() == FunctionErrors::CAN_THROW_RUNTIME_ERROR) {
			return false;
		}
		break;
	}
	case ExpressionClass::BOUND_FUNCTION: {
		auto &function = expr.Cast<BoundFunctionExpression>();
		// Lambda bodies are executable expressions stored outside the regular child list.
		if (function.Function().HasBindLambdaCallback()) {
			if (!function.BindInfo()) {
				return false;
			}
			auto lambda = function.BindInfo()->Cast<LambdaFunctionData>().GetLambdaExpression();
			if (!lambda || !ExpressionIsSafe(context, *lambda)) {
				return false;
			}
		}
		if (BoundCastExpression::IsCast(function)) {
			if (BoundCastExpression::CastCanThrow(BoundCastExpression::SourceType(function),
			                                      BoundCastExpression::TargetType(function),
			                                      BoundCastExpression::IsTryCast(function)) &&
			    !CanEvaluateFoldableExpression(context, expr)) {
				return false;
			}
		} else {
			if (function.Function().GetErrorMode() == FunctionErrors::CAN_THROW_RUNTIME_ERROR &&
			    !IsSystemFloatingPointArithmetic(function.Function()) &&
			    !CanEvaluateFoldableExpression(context, expr) && !TryGetBigIntRange(context, expr)) {
				return false;
			}
		}
		break;
	}
	case ExpressionClass::BOUND_WINDOW: {
		auto &window = expr.Cast<BoundWindowExpression>();
		if ((window.AggregateFunction() &&
		     window.AggregateFunction()->GetErrorMode() == FunctionErrors::CAN_THROW_RUNTIME_ERROR) ||
		    (window.WindowFunction() &&
		     window.WindowFunction()->GetErrorMode() == FunctionErrors::CAN_THROW_RUNTIME_ERROR)) {
			return false;
		}
		break;
	}
	case ExpressionClass::BOUND_CASE:
	case ExpressionClass::BOUND_COLUMN_REF:
	case ExpressionClass::BOUND_CONJUNCTION:
	case ExpressionClass::BOUND_CONSTANT:
	case ExpressionClass::BOUND_OPERATOR:
	case ExpressionClass::BOUND_PARAMETER:
	case ExpressionClass::BOUND_REF:
	case ExpressionClass::BOUND_UNNEST:
		break;
	default:
		return false;
	}

	bool safe = true;
	ExpressionIterator::EnumerateChildren(expr,
	                                      [&](const Expression &child) { safe &= ExpressionIsSafe(context, child); });
	return safe;
}

static bool OperatorExpressionsAreSafe(ClientContext &context, const LogicalOperator &op) {
	bool safe = true;
	LogicalOperatorVisitor::EnumerateExpressions(op, [&](const unique_ptr<Expression> *expression) {
		if (expression && *expression) {
			safe &= ExpressionIsSafe(context, **expression);
		}
	});
	return safe;
}

static bool TableFiltersAreSafe(ClientContext &context, const LogicalGet &get) {
	for (const auto &entry : get.table_filters) {
		auto &filter = ExpressionFilter::GetExpressionFilter(entry.Filter(), "DuplicateEliminatedDomainSafety");
		if (!ExpressionIsSafe(context, *filter.expr)) {
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

static bool ScanIsSafe(ClientContext &context, const LogicalGet &get) {
	return IsSupportedScan(get) && TableFiltersAreSafe(context, get);
}

static bool CanPreparePayloadInternal(ClientContext &context, const LogicalOperator &op) {
	if (!OperatorExpressionsAreSafe(context, op)) {
		return false;
	}
	switch (op.type) {
	case LogicalOperatorType::LOGICAL_GET:
		if (!ScanIsSafe(context, op.Cast<LogicalGet>())) {
			return false;
		}
		break;
	case LogicalOperatorType::LOGICAL_CTE_REF:
	case LogicalOperatorType::LOGICAL_EXPRESSION_GET:
	case LogicalOperatorType::LOGICAL_DUMMY_SCAN:
	case LogicalOperatorType::LOGICAL_EMPTY_RESULT:
		break;
	case LogicalOperatorType::LOGICAL_FILTER:
	case LogicalOperatorType::LOGICAL_PROJECTION:
	case LogicalOperatorType::LOGICAL_AGGREGATE_AND_GROUP_BY:
	case LogicalOperatorType::LOGICAL_CROSS_PRODUCT:
		break;
	case LogicalOperatorType::LOGICAL_COMPARISON_JOIN: {
		auto &join = op.Cast<LogicalComparisonJoin>();
		if (join.HasProjectionMap() || (join.join_type != JoinType::INNER && join.join_type != JoinType::SEMI)) {
			return false;
		}
		break;
	}
	default:
		return false;
	}
	for (auto &child : op.children) {
		if (!CanPreparePayloadInternal(context, *child)) {
			return false;
		}
	}
	return true;
}

bool DuplicateEliminatedDomainSafety::CanPreparePayload(ClientContext &context, const LogicalOperator &op) {
	return CanPreparePayloadInternal(context, op);
}

static bool CanEvaluateAdditionalGroupsForScan(ClientContext &context, const LogicalGet &get) {
	// Domain elimination does not duplicate a leaf scan. It only allows more of that scan's rows to reach the
	// downstream operators, whose expressions are checked separately.
	return ScanIsSafe(context, get);
}

class AdditionalGroupAnalyzer {
public:
	AdditionalGroupAnalyzer(ClientContext &context_p, LogicalOperator &root,
	                        const DuplicateEliminatedDomainCTERegistry &cte_registry_p, TableIndex domain_cte_index_p)
	    : context(context_p), rewrite_root(root), cte_registry(cte_registry_p), domain_cte_index(domain_cte_index_p) {
	}

	bool CanEvaluate(LogicalOperator &root) {
		unordered_set<TableIndex> visiting_ctes;
		return SubtreeIsInspectable(root, visiting_ctes);
	}

private:
	bool SubtreeIsInspectable(LogicalOperator &op, unordered_set<TableIndex> &visiting_ctes) const {
		if (!OperatorExpressionsAreSafe(context, op)) {
			return false;
		}
		switch (op.type) {
		case LogicalOperatorType::LOGICAL_GET: {
			auto &get = op.Cast<LogicalGet>();
			if (!CanEvaluateAdditionalGroupsForScan(context, get)) {
				return false;
			}
			break;
		}
		case LogicalOperatorType::LOGICAL_COMPARISON_JOIN:
		case LogicalOperatorType::LOGICAL_ANY_JOIN:
		case LogicalOperatorType::LOGICAL_ASOF_JOIN:
			if (op.Cast<LogicalJoin>().join_type == JoinType::SINGLE &&
			    (op.type != LogicalOperatorType::LOGICAL_COMPARISON_JOIN ||
			     !DuplicateEliminatedDomainProperties::SingleJoinRHSIsDeduplicated(op.Cast<LogicalComparisonJoin>(),
			                                                                       rewrite_root))) {
				return false;
			}
			break;
		case LogicalOperatorType::LOGICAL_CTE_REF: {
			auto &cte_ref = op.Cast<LogicalCTERef>();
			if (cte_ref.cte_index == domain_cte_index) {
				return true;
			}
			if (cte_ref.is_recurring) {
				return false;
			}
			auto cte_definition = cte_registry.FindDefinition(cte_ref.cte_index);
			if (!cte_definition) {
				return false;
			}
			if (cte_registry.IsAlwaysMaterialized(cte_ref.cte_index)) {
				return true;
			}
			if (!visiting_ctes.insert(cte_ref.cte_index).second) {
				return false;
			}
			auto result = SubtreeIsInspectable(*cte_definition, visiting_ctes);
			visiting_ctes.erase(cte_ref.cte_index);
			return result;
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
		case LogicalOperatorType::LOGICAL_CROSS_PRODUCT:
		case LogicalOperatorType::LOGICAL_CHUNK_GET:
		case LogicalOperatorType::LOGICAL_DUMMY_SCAN:
		case LogicalOperatorType::LOGICAL_EMPTY_RESULT:
			break;
		default:
			return false;
		}
		for (auto &child : op.children) {
			if (!SubtreeIsInspectable(*child, visiting_ctes)) {
				return false;
			}
		}
		return true;
	}

private:
	ClientContext &context;
	LogicalOperator &rewrite_root;
	const DuplicateEliminatedDomainCTERegistry &cte_registry;
	TableIndex domain_cte_index;
};

bool DuplicateEliminatedDomainSafety::CanEvaluateAdditionalGroups(
    ClientContext &context, LogicalOperator &rewrite_root, const DuplicateEliminatedDomainCTERegistry &cte_registry,
    LogicalOperator &op, TableIndex domain_cte_index) {
	AdditionalGroupAnalyzer analyzer(context, rewrite_root, cte_registry, domain_cte_index);
	return analyzer.CanEvaluate(op);
}

bool DuplicateEliminatedDomainSafety::CanFactorOperator(ClientContext &context, const LogicalOperator &op) {
	if (!OperatorExpressionsAreSafe(context, op)) {
		return false;
	}
	switch (op.type) {
	case LogicalOperatorType::LOGICAL_GET: {
		auto &get = op.Cast<LogicalGet>();
		return ScanIsSafe(context, get);
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
		if (op.children.size() != 2 || (join.join_type != JoinType::INNER && join.join_type != JoinType::SEMI) ||
		    (join.join_type == JoinType::SEMI && join.HasProjectionMap())) {
			return false;
		}
		break;
	}
	default:
		return false;
	}
	return true;
}

static bool CanDuplicateSourceInternal(ClientContext &context, const LogicalOperator &op) {
	if (!OperatorExpressionsAreSafe(context, op)) {
		return false;
	}
	switch (op.type) {
	case LogicalOperatorType::LOGICAL_GET: {
		auto &get = op.Cast<LogicalGet>();
		return ScanIsSafe(context, get);
	}
	case LogicalOperatorType::LOGICAL_FILTER:
	case LogicalOperatorType::LOGICAL_PROJECTION:
		return op.children.size() == 1 && CanDuplicateSourceInternal(context, *op.children[0]);
	default:
		return false;
	}
}

bool DuplicateEliminatedDomainSafety::CanDuplicateSource(ClientContext &context, LogicalOperator &op) {
	return LogicalSubtreeIsRepeatable(op) && CanDuplicateSourceInternal(context, op);
}

} // namespace duckdb
