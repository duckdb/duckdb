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

struct WindowSQLFrame {
	optional_ptr<const Expression> order;
	optional_ptr<const Expression> start;
	optional_ptr<const Expression> end;
	unique_ptr<Expression> start_literal;
	unique_ptr<Expression> end_literal;
};

static optional<Value> NumericRangeLiteral(const ParsedExpression &literal) {
	if (literal.GetExpressionClass() == ExpressionClass::CONSTANT) {
		auto value = literal.Cast<ConstantExpression>().GetLiteral().ToValue();
		return value.type().IsNumeric() && !value.IsNull() ? optional<Value>(value) : optional<Value>();
	}
	if (literal.GetExpressionClass() == ExpressionClass::CAST) {
		auto &cast = literal.Cast<CastExpression>();
		auto type = UnboundType::TryDefaultBind(cast.TargetType());
		auto child = NumericRangeLiteral(cast.Child());
		if (child && type.IsNumeric() && !cast.IsTryCast()) {
			return child->DefaultTryCastAs(type);
		}
	}
	return {};
}

static WindowSQLFrame ReconstructWindowFrame(const BoundWindowExpression &expression) {
	auto is_range_offset = [](WindowBoundary boundary) {
		return boundary == WindowBoundary::EXPR_PRECEDING_RANGE || boundary == WindowBoundary::EXPR_FOLLOWING_RANGE;
	};
	WindowSQLFrame frame;
	if (expression.OrderBy().size() == 1) {
		frame.order = WindowRangeCast::Match(*expression.OrderBy()[0].expression, expression.SQLRangeOrderCasts());
		if (frame.order && expression.SQLRangeOrderType().IsComplete() &&
		    !frame.order->GetReturnType().EqualsIncludingCollation(expression.SQLRangeOrderType())) {
			frame.order = nullptr;
		}
	}
	auto range_offset = [&](const unique_ptr<Expression> &endpoint, WindowBoundary boundary,
	                        const unique_ptr<Expression> &literal,
	                        const unique_ptr<WindowRangeBoundary> &origin) -> optional_ptr<const Expression> {
		if (!is_range_offset(boundary)) {
			return endpoint.get();
		}
		if (!origin || !endpoint || !frame.order || origin->boundary != boundary ||
		    origin->direction != expression.OrderBy()[0].type) {
			return nullptr;
		}
		const bool has_literal_offset = literal && literal->GetExpressionClass() == ExpressionClass::BOUND_CONSTANT;
		const bool has_retained_endpoint = has_literal_offset && endpoint && frame.order;
		const bool endpoint_is_order =
		    has_retained_endpoint && Expression::Equals(*endpoint, *expression.OrderBy()[0].expression);
		if (endpoint_is_order) {
			auto &value = literal->Cast<BoundConstantExpression>().GetValue();
			if (!value.IsNull() && value.type().IsNumeric() && value == Value::Numeric(value.type(), 0)) {
				return literal.get();
			}
		}
		auto input = origin->Match(*endpoint, *frame.order);
		if (!input) {
			return nullptr;
		}
		auto &offset = *input;
		if (offset.GetExpressionClass() == ExpressionClass::BOUND_CONSTANT &&
		    offset.Cast<BoundConstantExpression>().GetValue().IsNull()) {
			return nullptr;
		}
		if (literal && literal->GetExpressionClass() == ExpressionClass::BOUND_CONSTANT &&
		    offset.GetExpressionClass() == ExpressionClass::BOUND_CONSTANT) {
			auto &original = literal->Cast<BoundConstantExpression>().GetValue();
			auto &current = offset.Cast<BoundConstantExpression>().GetValue();
			const bool has_numeric_offsets = original.type().IsNumeric() && current.type().IsNumeric();
			if (!original.IsNull() && has_numeric_offsets && original == current) {
				return literal.get();
			}
		}
		return &offset;
	};
	auto retained_offset = [&](const unique_ptr<ParsedExpression> &literal) -> unique_ptr<Expression> {
		auto value = literal ? NumericRangeLiteral(*literal) : optional<Value>();
		return value ? make_uniq<BoundConstantExpression>(*value) : nullptr;
	};
	frame.start_literal = retained_offset(expression.SQLRangeStart());
	frame.end_literal = retained_offset(expression.SQLRangeEnd());
	frame.start = range_offset(expression.StartExpr(), expression.WindowStart(), frame.start_literal,
	                           expression.SQLRangeStartBoundary());
	frame.end =
	    range_offset(expression.EndExpr(), expression.WindowEnd(), frame.end_literal, expression.SQLRangeEndBoundary());
	return frame;
}

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
	D_ASSERT(definition);
	auto identity =
	    DefinitionFunctionIdentity(*definition, function.GetLogicalArguments(), function.GetLogicalReturnType());
	if (!identity.IsValid()) {
		return BoundExpressionSQLExportResult::Failure(
		    {InternalExpressionInvariant(path, expression, "Bound window function identity is incomplete")});
	}
	if (expression.GetChildren().size() != function.GetLogicalArguments().size()) {
		return BoundExpressionSQLExportResult::Failure(
		    {UnsupportedFunction(path, std::move(identity), "The window function does not retain every SQL argument")});
	}
	auto name = RebindableFunctionName(*definition);
	if (!name || !IsSQLValueType(expression.GetReturnType()) ||
	    definition->GetProperties().GetCaptureArgumentAliases()) {
		return BoundExpressionSQLExportResult::Failure({UnsupportedFunction(
		    path, std::move(identity), "The window function no longer represents its logical SQL signature")});
	}
	auto frame = ReconstructWindowFrame(expression);
	if ((expression.StartExpr() && !frame.start) || (expression.EndExpr() && !frame.end)) {
		return BoundExpressionSQLExportResult::Failure({UnsupportedFeature(
		    path, "window_range_offset", "The RANGE endpoint does not retain its SQL offset and ordering operand")});
	}

	vector<ChildExpression> source_children;
	for (auto &partition : expression.Partitions()) {
		source_children.emplace_back(partition.get());
	}
	for (auto &order : expression.OrderBy()) {
		source_children.emplace_back(frame.order ? frame.order : order.expression.get());
	}
	for (idx_t i = 0; i < expression.GetChildren().size(); i++) {
		source_children.emplace_back(expression.GetChildren()[i].get());
	}
	if (expression.Filter()) {
		source_children.emplace_back(expression.Filter().get(), LogicalType::BOOLEAN);
	}
	if (expression.StartExpr()) {
		source_children.emplace_back(frame.start);
	}
	if (expression.EndExpr()) {
		source_children.emplace_back(frame.end);
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
		window->OrderByMutable().emplace_back(
		    order.type, order.null_order,
		    SQLExportHelpers::OrderExpression(order.expression->GetReturnType(), std::move(children[ordinal++])));
	}
	auto &named_arguments = function.GetNamedArguments();
	auto positional_count = function.GetPositionalArgumentCount();
	if (!named_arguments.empty() && positional_count + named_arguments.size() != expression.GetChildren().size()) {
		return BoundExpressionSQLExportResult::Failure(
		    {UnsupportedFunction(path, std::move(identity), "The named SQL arguments are incomplete")});
	}
	for (idx_t i = 0; i < expression.GetChildren().size(); i++) {
		auto argument_name =
		    !named_arguments.empty() && i >= positional_count ? named_arguments[i - positional_count] : Identifier();
		window->GetArgumentsMutable().emplace_back(std::move(argument_name), std::move(children[ordinal++]));
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
		window->ArgOrdersMutable().emplace_back(
		    order.type, order.null_order,
		    SQLExportHelpers::OrderExpression(order.expression->GetReturnType(), std::move(children[ordinal++])));
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
