#include "transformer/peg_transformer.hpp"
#include "duckdb/parser/expression/comparison_expression.hpp"
#include "duckdb/parser/expression/between_expression.hpp"
#include "duckdb/parser/expression/operator_expression.hpp"
#include "duckdb/parser/expression/cast_expression.hpp"

namespace duckdb {

// BaseExpression <- SingleExpression Indirection*
unique_ptr<ParsedExpression> PEGTransformerFactory::TransformBaseExpression(PEGTransformer &transformer,
                                                                            optional_ptr<ParseResult> parse_result) {
	auto &list_pr = parse_result->Cast<ListParseResult>();
	auto expr = transformer.Transform<unique_ptr<ParsedExpression>>(list_pr.Child<ListParseResult>(0));
	auto indirection_opt = list_pr.Child<OptionalParseResult>(1);
	if (indirection_opt.HasResult()) {
		auto indirection_repeat = indirection_opt.optional_result->Cast<RepeatParseResult>();
		for (auto child : indirection_repeat.children) {
			auto indirection_expr = transformer.Transform<unique_ptr<ParsedExpression>>(child);
			if (indirection_expr->GetExpressionClass() == ExpressionClass::CAST) {
				auto cast_expr = unique_ptr_cast<ParsedExpression, CastExpression>(std::move(indirection_expr));
				cast_expr->child = std::move(expr);
				expr = std::move(cast_expr);
			} else if (indirection_expr->GetExpressionClass() == ExpressionClass::OPERATOR) {
				auto operator_expr = unique_ptr_cast<ParsedExpression, OperatorExpression>(std::move(indirection_expr));
				operator_expr->children.insert(operator_expr->children.begin(), std::move(expr));
				expr = std::move(operator_expr);
			} else if (indirection_expr->GetExpressionClass() == ExpressionClass::FUNCTION) {
				auto function_expr = unique_ptr_cast<ParsedExpression, FunctionExpression>(std::move(indirection_expr));
				function_expr->children.push_back(std::move(expr));
				expr = std::move(function_expr);
			}
		}
	}

	return expr;
}

unique_ptr<ColumnRefExpression>
PEGTransformerFactory::TransformNestedColumnName(PEGTransformer &transformer, optional_ptr<ParseResult> parse_result) {
	auto &list_pr = parse_result->Cast<ListParseResult>();
	vector<string> column_names;
	auto opt_identifiers = list_pr.Child<OptionalParseResult>(0);
	if (opt_identifiers.HasResult()) {
		auto repeat_identifiers = opt_identifiers.optional_result->Cast<RepeatParseResult>();
		for (auto &child : repeat_identifiers.children) {
			column_names.push_back(transformer.Transform<string>(child));
		}
	}
	column_names.push_back(list_pr.Child<IdentifierParseResult>(1).identifier);
	return make_uniq<ColumnRefExpression>(std::move(column_names));
}

// ColumnReference <- CatalogReservedSchemaTableColumnName / SchemaReservedTableColumnName / TableReservedColumnName /
// NestedColumnName
unique_ptr<ParsedExpression> PEGTransformerFactory::TransformColumnReference(PEGTransformer &transformer,
                                                                             optional_ptr<ParseResult> parse_result) {
	auto &list_pr = parse_result->Cast<ListParseResult>();
	return transformer.Transform<unique_ptr<ColumnRefExpression>>(list_pr.Child<ChoiceParseResult>(0).result);
}

unique_ptr<ParsedExpression>
PEGTransformerFactory::TransformFunctionExpression(PEGTransformer &transformer,
                                                   optional_ptr<ParseResult> parse_result) {
	auto &list_pr = parse_result->Cast<ListParseResult>();
	auto qualified_function = transformer.Transform<QualifiedName>(list_pr.Child<ListParseResult>(0));
	auto extract_parens = ExtractResultFromParens(list_pr.Child<ListParseResult>(1))->Cast<ListParseResult>();
	bool distinct = false;
	transformer.TransformOptional<bool>(extract_parens, 0, distinct);
	auto function_arg_opt = extract_parens.Child<OptionalParseResult>(1);
	vector<unique_ptr<ParsedExpression>> function_children;
	if (function_arg_opt.HasResult()) {
		auto function_argument_list = ExtractParseResultsFromList(function_arg_opt.optional_result);
		for (auto function_argument : function_argument_list) {
			function_children.push_back(transformer.Transform<unique_ptr<ParsedExpression>>(function_argument));
		}
	}

	if (function_children.size() == 1 && ExpressionIsEmptyStar(*function_children[0])) {
		// COUNT(*) gets converted into COUNT()
		function_children.clear();
	}

	vector<OrderByNode> order_by;
	transformer.TransformOptional<vector<OrderByNode>>(list_pr, 2, order_by);
	auto ignore_nulls_opt = extract_parens.Child<OptionalParseResult>(3);
	if (ignore_nulls_opt.HasResult()) {
		throw NotImplementedException("Ignore nulls has not yet been implemented");
	}
	auto within_group_opt = list_pr.Child<OptionalParseResult>(2);
	if (within_group_opt.HasResult()) {
		throw NotImplementedException("Within group has not yet been implemented");
	}
	unique_ptr<ParsedExpression> filter_expr;
	transformer.TransformOptional<unique_ptr<ParsedExpression>>(list_pr, 3, filter_expr);
	auto export_opt = list_pr.Child<OptionalParseResult>(4);
	if (export_opt.HasResult()) {
		throw NotImplementedException("Export has not yet been implemented");
	}
	auto over_opt = list_pr.Child<OptionalParseResult>(5);
	if (over_opt.HasResult()) {
		auto window_function = transformer.Transform<unique_ptr<WindowExpression>>(over_opt.optional_result);
		window_function->catalog = qualified_function.catalog;
		window_function->schema = qualified_function.schema;
		window_function->function_name = qualified_function.name;
		window_function->children = std::move(function_children);
		window_function->type = WindowExpression::WindowToExpressionType(window_function->function_name);
		return std::move(window_function);
	}

	auto result = make_uniq<FunctionExpression>(qualified_function.catalog, qualified_function.schema,
	                                            qualified_function.name, std::move(function_children));

	result->distinct = distinct;
	if (!order_by.empty()) {
		auto order_by_modifier = make_uniq<OrderModifier>();
		order_by_modifier->orders = std::move(order_by);
		result->order_bys = std::move(order_by_modifier);
	}
	if (filter_expr) {
		result->filter = std::move(filter_expr);
	}
	return std::move(result);
}

QualifiedName PEGTransformerFactory::TransformFunctionIdentifier(PEGTransformer &transformer,
                                                                 optional_ptr<ParseResult> parse_result) {
	auto &list_pr = parse_result->Cast<ListParseResult>();
	auto choice_pr = list_pr.Child<ChoiceParseResult>(0);
	if (choice_pr.result->type == ParseResultType::IDENTIFIER) {
		QualifiedName result;
		result.catalog = INVALID_CATALOG;
		result.schema = INVALID_SCHEMA;
		result.name = choice_pr.result->Cast<IdentifierParseResult>().identifier;
		return result;
	}
	return transformer.Transform<QualifiedName>(list_pr.Child<ChoiceParseResult>(0).result);
}

// Expression <- BaseExpression RecursiveExpression*
unique_ptr<ParsedExpression> PEGTransformerFactory::TransformExpression(PEGTransformer &transformer,
                                                                        optional_ptr<ParseResult> parse_result) {
	auto &list_pr = parse_result->Cast<ListParseResult>();
	auto &base_expr_pr = list_pr.Child<ListParseResult>(0);
	unique_ptr<ParsedExpression> base_expr = transformer.Transform<unique_ptr<ParsedExpression>>(base_expr_pr);
	auto &indirection_pr = list_pr.Child<OptionalParseResult>(1);
	if (indirection_pr.HasResult()) {
		auto repeat_expression_pr = indirection_pr.optional_result->Cast<RepeatParseResult>();
		vector<unique_ptr<ParsedExpression>> expr_children;
		for (auto &child : repeat_expression_pr.children) {
			auto expr = transformer.Transform<unique_ptr<ParsedExpression>>(child);
			if (expr->expression_class == ExpressionClass::COMPARISON) {
				auto compare_expr = unique_ptr_cast<ParsedExpression, ComparisonExpression>(std::move(expr));
				compare_expr->left = std::move(base_expr);
				base_expr = std::move(compare_expr);
			} else if (expr->expression_class == ExpressionClass::FUNCTION) {
				auto func_expr = unique_ptr_cast<ParsedExpression, FunctionExpression>(std::move(expr));
				func_expr->children.insert(func_expr->children.begin(), std::move(base_expr));
				base_expr = std::move(func_expr);
			} else if (expr->expression_class == ExpressionClass::LAMBDA) {
				auto lambda_expr = unique_ptr_cast<ParsedExpression, LambdaExpression>(std::move(expr));
				lambda_expr->lhs = std::move(base_expr);
				base_expr = std::move(lambda_expr);
			} else if (expr->expression_class == ExpressionClass::BETWEEN) {
				auto between_expr = unique_ptr_cast<ParsedExpression, BetweenExpression>(std::move(expr));
				between_expr->input = std::move(base_expr);
				base_expr = std::move(between_expr);
			} else {
				base_expr = make_uniq<OperatorExpression>(expr->type, std::move(base_expr), std::move(expr));
			}
		}
	}

	return base_expr;
}

// LiteralExpression <- StringLiteral / NumberLiteral / 'NULL' / 'TRUE' / 'FALSE'
unique_ptr<ParsedExpression> PEGTransformerFactory::TransformLiteralExpression(PEGTransformer &transformer,
                                                                               optional_ptr<ParseResult> parse_result) {
	auto &choice_result = parse_result->Cast<ListParseResult>();
	auto &matched_rule_result = choice_result.Child<ChoiceParseResult>(0);
	if (matched_rule_result.name == "StringLiteral") {
		return make_uniq<ConstantExpression>(Value(transformer.Transform<string>(matched_rule_result.result)));
	}
	return transformer.Transform<unique_ptr<ParsedExpression>>(matched_rule_result.result);
}

unique_ptr<ParsedExpression> PEGTransformerFactory::TransformConstantLiteral(PEGTransformer &transformer,
                                                                             optional_ptr<ParseResult> parse_result) {
	auto &list_pr = parse_result->Cast<ListParseResult>();
	return make_uniq<ConstantExpression>(transformer.TransformEnum<Value>(list_pr.Child<ChoiceParseResult>(0).result));
}

// SingleExpression <- LiteralExpression /
// Parameter /
// SubqueryExpression /
// SpecialFunctionExpression /
// ParenthesisExpression /
// IntervalLiteral /
// TypeLiteral /
// CaseExpression /
// StarExpression /
// CastExpression /
// GroupingExpression /
// MapExpression /
// FunctionExpression /
// ColumnReference /
// PrefixExpression /
// ListComprehensionExpression /
// ListExpression /
// StructExpression /
// PositionalExpression /
// DefaultExpression
unique_ptr<ParsedExpression> PEGTransformerFactory::TransformSingleExpression(PEGTransformer &transformer,
                                                                              optional_ptr<ParseResult> parse_result) {
	auto &list_pr = parse_result->Cast<ListParseResult>();
	return transformer.Transform<unique_ptr<ParsedExpression>>(list_pr.Child<ChoiceParseResult>(0).result);
}

} // namespace duckdb
