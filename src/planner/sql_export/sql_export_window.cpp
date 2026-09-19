#include "duckdb/planner/sql_export/bound_expression_sql_exporter_internal.hpp"
#include "duckdb/planner/expression/bound_aggregate_expression.hpp"
#include "duckdb/planner/sql_export_helpers.hpp"
#include "duckdb/parser/expression/cast_expression.hpp"
#include "duckdb/parser/expression/constant_expression.hpp"
#include "duckdb/planner/expression/bound_cast_expression.hpp"
#include "duckdb/planner/expression/bound_constant_expression.hpp"
#include "duckdb/planner/expression/bound_function_expression.hpp"
#include "duckdb/function/window_function.hpp"
#include "duckdb/planner/expression/bound_window_expression.hpp"

namespace duckdb {
namespace bound_expression_sql_export {

BoundExpressionSQLExportResult BoundExpressionSQLExportState::ExportWindow(const BoundWindowExpression &expression,
                                                                           const LogicalPlanVerificationPath &path) {
	if (expression.AggregateFunction()) {
		return PreserveCollation(expression.GetReturnType(),
		                         ExportWindowFunction(expression, *expression.AggregateFunction(), path), path);
	}
	D_ASSERT(expression.WindowFunction());
	return PreserveCollation(expression.GetReturnType(),
	                         ExportWindowFunction(expression, *expression.WindowFunction(), path), path);
}

template <class FUNCTION>
BoundExpressionSQLExportResult
BoundExpressionSQLExportState::ExportWindowFunction(const BoundWindowExpression &expression, const FUNCTION &function,
                                                    const LogicalPlanVerificationPath &path) {
	auto &definition = function.GetDefinition();
	if (!definition) {
		return Failure(
		    InternalExpressionInvariant(path, expression, "Bound window function has no retained definition"));
	}
	auto identity =
	    DefinitionFunctionIdentity(*definition, function.GetLogicalArguments(), function.GetLogicalReturnType());
	if (!identity.IsValid()) {
		return Failure(InternalExpressionInvariant(path, expression, "Bound window function identity is incomplete"));
	}
	if (expression.GetChildren().size() != function.GetLogicalArguments().size()) {
		return Failure(
		    UnsupportedFunction(path, std::move(identity), "The window function does not retain every SQL argument"));
	}
	auto name = RebindableFunctionName(*definition);
	if (!name || !IsSQLValueType(expression.GetReturnType()) ||
	    definition->GetProperties().GetCaptureArgumentAliases()) {
		return Failure(UnsupportedFunction(path, std::move(identity),
		                                   "The window function no longer represents its logical SQL signature"));
	}
	auto is_range_offset = [](WindowBoundary boundary) {
		return boundary == WindowBoundary::EXPR_PRECEDING_RANGE || boundary == WindowBoundary::EXPR_FOLLOWING_RANGE;
	};
	optional_ptr<const Expression> sql_order;
	if (expression.OrderBy().size() == 1) {
		sql_order = expression.OrderBy()[0].expression.get();
		if (sql_order && expression.SQLRangeOrderType().IsComplete() &&
		    !sql_order->GetReturnType().EqualsIncludingCollation(expression.SQLRangeOrderType()) &&
		    BoundCastExpression::IsCast(*sql_order)) {
			auto &cast = sql_order->Cast<BoundFunctionExpression>();
			if (BoundCastExpression::HasValidBindData(cast) && !BoundCastExpression::IsTryCast(cast) &&
			    cast.GetChildren().size() == 1 && cast.GetChildren()[0] &&
			    cast.GetChildren()[0]->GetReturnType().EqualsIncludingCollation(expression.SQLRangeOrderType())) {
				sql_order = cast.GetChildren()[0].get();
			}
		}
	}
	auto range_offset = [&](const unique_ptr<Expression> &endpoint, WindowBoundary boundary,
	                        const unique_ptr<Expression> &literal) -> optional_ptr<const Expression> {
		if (!is_range_offset(boundary)) {
			return endpoint.get();
		}
		if (endpoint && sql_order && literal && literal->GetExpressionClass() == ExpressionClass::BOUND_CONSTANT &&
		    Expression::Equals(*endpoint, *expression.OrderBy()[0].expression)) {
			auto &value = literal->Cast<BoundConstantExpression>().GetValue();
			if (!value.IsNull() && value.type().IsNumeric() && value == Value::Numeric(value.type(), 0)) {
				return literal.get();
			}
		}
		if (!endpoint || expression.OrderBy().size() != 1 ||
		    endpoint->GetExpressionClass() != ExpressionClass::BOUND_FUNCTION) {
			return nullptr;
		}
		auto &arithmetic = endpoint->Cast<BoundFunctionExpression>();
		auto &order = expression.OrderBy()[0];
		const bool subtract =
		    (boundary == WindowBoundary::EXPR_PRECEDING_RANGE) == (order.type == OrderType::ASCENDING);
		auto definition = arithmetic.Function().GetDefinition();
		auto arithmetic_name = definition ? RebindableFunctionName(*definition) : optional<QualifiedName>();
		if (!arithmetic_name || *arithmetic_name != QualifiedName("system", "main", subtract ? "-" : "+") ||
		    arithmetic.GetChildren().size() != 2 || !arithmetic.GetChildren()[0] || !arithmetic.GetChildren()[1] ||
		    !order.expression || (order.type != OrderType::ASCENDING && order.type != OrderType::DESCENDING) ||
		    endpoint->GetReturnType() != order.expression->GetReturnType() || !sql_order ||
		    (!Expression::Equals(*arithmetic.GetChildren()[0], *sql_order) &&
		     !Expression::Equals(*arithmetic.GetChildren()[0], *order.expression))) {
			return nullptr;
		}
		auto &offset = *arithmetic.GetChildren()[1];
		if (offset.GetExpressionClass() == ExpressionClass::BOUND_CONSTANT &&
		    offset.Cast<BoundConstantExpression>().GetValue().IsNull()) {
			return nullptr;
		}
		if (literal && literal->GetExpressionClass() == ExpressionClass::BOUND_CONSTANT &&
		    offset.GetExpressionClass() == ExpressionClass::BOUND_CONSTANT) {
			auto &original = literal->Cast<BoundConstantExpression>().GetValue();
			auto &current = offset.Cast<BoundConstantExpression>().GetValue();
			if (!original.IsNull() && original.type().IsNumeric() && current.type().IsNumeric() &&
			    original == current) {
				return literal.get();
			}
		}
		return &offset;
	};
	std::function<optional<Value>(const ParsedExpression &)> numeric_literal =
	    [&](const ParsedExpression &literal) -> optional<Value> {
		if (literal.GetExpressionClass() == ExpressionClass::CONSTANT) {
			auto value = literal.Cast<ConstantExpression>().GetLiteral().ToValue();
			return value.type().IsNumeric() && !value.IsNull() ? optional<Value>(value) : optional<Value>();
		}
		if (literal.GetExpressionClass() == ExpressionClass::CAST) {
			auto &cast = literal.Cast<CastExpression>();
			auto type = UnboundType::TryDefaultBind(cast.TargetType());
			auto child = numeric_literal(cast.Child());
			if (child && type.IsNumeric() && !cast.IsTryCast()) {
				return child->DefaultTryCastAs(type);
			}
		}
		return {};
	};
	auto retained_offset = [&](const unique_ptr<ParsedExpression> &literal) -> unique_ptr<Expression> {
		auto value = literal ? numeric_literal(*literal) : optional<Value>();
		return value ? make_uniq<BoundConstantExpression>(*value) : nullptr;
	};
	auto start_literal = retained_offset(expression.SQLRangeStart());
	auto end_literal = retained_offset(expression.SQLRangeEnd());
	auto start = range_offset(expression.StartExpr(), expression.WindowStart(), start_literal);
	auto end = range_offset(expression.EndExpr(), expression.WindowEnd(), end_literal);
	if ((expression.StartExpr() && !start) || (expression.EndExpr() && !end)) {
		return Failure(UnsupportedFeature(path, "window_range_offset",
		                                  "The RANGE endpoint does not retain its SQL offset and ordering operand"));
	}

	vector<ChildExpression> source_children;
	for (auto &partition : expression.Partitions()) {
		source_children.emplace_back(partition.get());
	}
	for (auto &order : expression.OrderBy()) {
		source_children.emplace_back(sql_order ? sql_order : order.expression.get());
	}
	for (idx_t i = 0; i < expression.GetChildren().size(); i++) {
		source_children.emplace_back(expression.GetChildren()[i].get());
	}
	if (expression.Filter()) {
		source_children.emplace_back(expression.Filter().get(), LogicalType::BOOLEAN);
	}
	if (expression.StartExpr()) {
		source_children.emplace_back(start);
	}
	if (expression.EndExpr()) {
		source_children.emplace_back(end);
	}
	for (auto &order : expression.ArgOrders()) {
		source_children.emplace_back(order.expression.get());
	}
	vector<unique_ptr<ParsedExpression>> children;
	vector<LogicalPlanVerificationIssue> issues;
	ExportChildren(source_children, path, children, issues);
	if (!issues.empty()) {
		return BoundExpressionSQLExportResult::Failure(std::move(issues));
	}
	auto window = make_uniq<WindowExpression>(name->Catalog().GetIdentifierName(), name->Schema().GetIdentifierName(),
	                                          name->Name().GetIdentifierName());
	window->SetQualifiedName(*name);
	idx_t ordinal = 0;
	for (idx_t i = 0; i < expression.Partitions().size(); i++) {
		window->PartitionsMutable().push_back(std::move(children[ordinal++]));
	}
	for (auto &order : expression.OrderBy()) {
		window->OrderByMutable().emplace_back(order.type, order.null_order, std::move(children[ordinal++]));
	}
	for (idx_t i = 0; i < expression.GetChildren().size(); i++) {
		window->GetArgumentsMutable().emplace_back(std::move(children[ordinal++]));
	}
	if (expression.Filter()) {
		window->FilterMutable() = std::move(children[ordinal++]);
	}
	if (expression.StartExpr()) {
		window->StartExprMutable() = std::move(children[ordinal++]);
	}
	if (expression.EndExpr()) {
		window->EndExprMutable() = std::move(children[ordinal++]);
	}
	for (auto &order : expression.ArgOrders()) {
		window->ArgOrdersMutable().emplace_back(order.type, order.null_order, std::move(children[ordinal++]));
	}
	window->IgnoreNullsMutable() = expression.IgnoreNulls();
	window->HasIgnoreNullsMutable() = expression.IgnoreNulls();
	window->DistinctMutable() = expression.Distinct();
	window->WindowStartMutable() = expression.WindowStart();
	window->WindowEndMutable() = expression.WindowEnd();
	window->WindowExcludeMutable() = expression.WindowExclude();
	unique_ptr<ParsedExpression> result = std::move(window);
	if (IsSQLRepresentableType(expression.GetReturnType()) && definition->HasBindCallback() &&
	    definition->GetReturnType() != expression.GetReturnType()) {
		return RestoreResultType(expression.GetReturnType(), std::move(result), path);
	}
	return BoundExpressionSQLExportResult::Success(std::move(result));
}

} // namespace bound_expression_sql_export
} // namespace duckdb
