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
	if (!indirection_opt.HasResult()) {
		return expr;
	}

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

unique_ptr<ParsedExpression> PEGTransformerFactory::TransformPrefixExpression(PEGTransformer &transformer,
                                                                              optional_ptr<ParseResult> parse_result) {
	auto &list_pr = parse_result->Cast<ListParseResult>();
	auto prefix = transformer.Transform<string>(list_pr.Child<ListParseResult>(0));
	auto expr = transformer.Transform<unique_ptr<ParsedExpression>>(list_pr.Child<ListParseResult>(1));
	if (prefix == "NOT") {
		return make_uniq<OperatorExpression>(ExpressionType::OPERATOR_NOT, std::move(expr));
	}
	vector<unique_ptr<ParsedExpression>> expr_children;
	expr_children.push_back(std::move(expr));
	auto func_expr = make_uniq<FunctionExpression>(prefix, std::move(expr_children));
	func_expr->is_operator = true;
	return func_expr;
}

string PEGTransformerFactory::TransformPrefixOperator(PEGTransformer &transformer,
                                                      optional_ptr<ParseResult> parse_result) {
	auto &list_pr = parse_result->Cast<ListParseResult>();
	auto choice_pr = list_pr.Child<ChoiceParseResult>(0);
	return transformer.TransformEnum<string>(choice_pr.result);
}

unique_ptr<ParsedExpression> PEGTransformerFactory::TransformListExpression(PEGTransformer &transformer,
                                                                            optional_ptr<ParseResult> parse_result) {
	auto &list_pr = parse_result->Cast<ListParseResult>();
	return transformer.Transform<unique_ptr<ParsedExpression>>(list_pr.Child<ChoiceParseResult>(0).result);
}

unique_ptr<ParsedExpression>
PEGTransformerFactory::TransformArrayBoundedListExpression(PEGTransformer &transformer,
                                                           optional_ptr<ParseResult> parse_result) {
	auto &list_pr = parse_result->Cast<ListParseResult>();
	bool is_array = list_pr.Child<OptionalParseResult>(0).HasResult();
	auto list_expr = transformer.Transform<vector<unique_ptr<ParsedExpression>>>(list_pr.Child<ListParseResult>(1));
	if (!is_array) {
		return make_uniq<FunctionExpression>(INVALID_CATALOG, "main", "list_value", std::move(list_expr));
	}
	return make_uniq<OperatorExpression>(ExpressionType::ARRAY_CONSTRUCTOR, std::move(list_expr));
}

unique_ptr<ParsedExpression>
PEGTransformerFactory::TransformParenthesisExpression(PEGTransformer &transformer,
                                                      optional_ptr<ParseResult> parse_result) {
	// ParenthesisExpression <- Parens(List(Expression))
	vector<unique_ptr<ParsedExpression>> children;

	auto &list_pr = parse_result->Cast<ListParseResult>();
	auto expressions = ExtractParseResultsFromList(ExtractResultFromParens(list_pr.Child<ListParseResult>(0)));

	for (auto &expression : expressions) {
		children.push_back(transformer.Transform<unique_ptr<ParsedExpression>>(expression));
	}
	if (children.size() == 1) {
		return std::move(children[0]);
	}
	return make_uniq<FunctionExpression>(INVALID_CATALOG, DEFAULT_SCHEMA, "row", std::move(children));
}

unique_ptr<ParsedExpression> PEGTransformerFactory::TransformArrayParensSelect(PEGTransformer &transformer,
                                                                               optional_ptr<ParseResult> parse_result) {
	throw NotImplementedException("TransformArrayBoundedListExpression");
}

unique_ptr<ParsedExpression> PEGTransformerFactory::TransformStructExpression(PEGTransformer &transformer,
                                                                              optional_ptr<ParseResult> parse_result) {
	auto &list_pr = parse_result->Cast<ListParseResult>();
	auto struct_children_pr = list_pr.Child<OptionalParseResult>(1);

	auto func_name = "struct_pack";
	vector<unique_ptr<ParsedExpression>> struct_children;
	if (struct_children_pr.HasResult()) {
		auto struct_children_list = ExtractParseResultsFromList(struct_children_pr.optional_result);
		for (auto struct_child : struct_children_list) {
			struct_children.push_back(transformer.Transform<unique_ptr<ParsedExpression>>(struct_child));
		}
	}

	return make_uniq<FunctionExpression>(INVALID_CATALOG, "main", func_name, std::move(struct_children));
}

unique_ptr<ParsedExpression> PEGTransformerFactory::TransformStructField(PEGTransformer &transformer,
                                                                         optional_ptr<ParseResult> parse_result) {
	auto &list_pr = parse_result->Cast<ListParseResult>();
	auto alias = transformer.Transform<string>(list_pr.Child<ListParseResult>(0));
	auto expr = transformer.Transform<unique_ptr<ParsedExpression>>(list_pr.Child<ListParseResult>(2));
	expr->SetAlias(alias);
	return expr;
}

vector<unique_ptr<ParsedExpression>>
PEGTransformerFactory::TransformBoundedListExpression(PEGTransformer &transformer,
                                                      optional_ptr<ParseResult> parse_result) {
	auto &list_pr = parse_result->Cast<ListParseResult>();
	auto has_expr = list_pr.Child<OptionalParseResult>(1);
	vector<unique_ptr<ParsedExpression>> list_children;
	if (has_expr.HasResult()) {
		auto expr_list = ExtractParseResultsFromList(has_expr.optional_result);
		for (auto &expr : expr_list) {
			list_children.push_back(transformer.Transform<unique_ptr<ParsedExpression>>(expr));
		}
	}
	return list_children;
}

// Expression <- LogicalOrExpression
unique_ptr<ParsedExpression> PEGTransformerFactory::TransformExpression(PEGTransformer &transformer,
                                                                        optional_ptr<ParseResult> parse_result) {
	auto &list_pr = parse_result->Cast<ListParseResult>();
	return transformer.Transform<unique_ptr<ParsedExpression>>(list_pr.Child<ChoiceParseResult>(0).result);
}

// LogicalOrExpression <- LogicalAndExpression ('OR' LogicalAndExpression)*
unique_ptr<ParsedExpression> PEGTransformerFactory::TransformLogicalOrExpression(PEGTransformer &transformer,
                                                                        optional_ptr<ParseResult> parse_result) {
	auto &list_pr = parse_result->Cast<ListParseResult>();
	auto expr = transformer.Transform<unique_ptr<ParsedExpression>>(list_pr.Child<ListParseResult>(0));
	auto or_expr_opt = list_pr.Child<OptionalParseResult>(1);
	if (!or_expr_opt.HasResult()) {
		return expr;
	}
	auto or_expr_repeat = or_expr_opt.optional_result->Cast<RepeatParseResult>();
	for (auto &or_expr : or_expr_repeat.children) {
		auto &inner_list_pr = or_expr->Cast<ListParseResult>();
		auto right_expr = transformer.Transform<unique_ptr<ParsedExpression>>(inner_list_pr.Child<ListParseResult>(1));
		expr = make_uniq<OperatorExpression>(ExpressionType::CONJUNCTION_OR, std::move(expr), std::move(right_expr));
	}
	return expr;
}

// LogicalAndExpression <- LogicalNotExpression ('AND' LogicalNotExpression)*
unique_ptr<ParsedExpression> PEGTransformerFactory::TransformLogicalAndExpression(PEGTransformer &transformer,
                                                                        optional_ptr<ParseResult> parse_result) {
	auto &list_pr = parse_result->Cast<ListParseResult>();
	auto expr = transformer.Transform<unique_ptr<ParsedExpression>>(list_pr.Child<ListParseResult>(0));
	auto and_expr_opt = list_pr.Child<OptionalParseResult>(1);
	if (!and_expr_opt.HasResult()) {
		return expr;
	}
	auto and_expr_repeat = and_expr_opt.optional_result->Cast<RepeatParseResult>();
	for (auto &and_expr : and_expr_repeat.children) {
		auto &inner_list_pr = and_expr->Cast<ListParseResult>();
		auto right_expr = transformer.Transform<unique_ptr<ParsedExpression>>(inner_list_pr.Child<ListParseResult>(1));
		expr = make_uniq<OperatorExpression>(ExpressionType::CONJUNCTION_AND, std::move(expr), std::move(right_expr));
	}
	return expr;
}

// LogicalNotExpression <- 'NOT'* IsExpression
unique_ptr<ParsedExpression> PEGTransformerFactory::TransformLogicalNotExpression(PEGTransformer &transformer,
                                                                        optional_ptr<ParseResult> parse_result) {
	auto &list_pr = parse_result->Cast<ListParseResult>();
	auto expr = transformer.Transform<unique_ptr<ParsedExpression>>(list_pr.Child<ListParseResult>(1));
	auto not_expr_opt = list_pr.Child<OptionalParseResult>(0);
	if (!not_expr_opt.HasResult()) {
		return expr;
	}
	auto not_expr_repeat = not_expr_opt.optional_result->Cast<RepeatParseResult>();
	for (auto &_: not_expr_repeat.children) {
		vector<unique_ptr<ParsedExpression>> inner_list_children;
		inner_list_children.push_back(std::move(expr));
		expr = make_uniq<OperatorExpression>(ExpressionType::OPERATOR_NOT, std::move(inner_list_children));
	}
	return expr;
}

// IsExpression <- IsDistinctFromExpression (IsTest)*
unique_ptr<ParsedExpression> PEGTransformerFactory::TransformIsExpression(PEGTransformer &transformer,
                                                                        optional_ptr<ParseResult> parse_result) {
	auto &list_pr = parse_result->Cast<ListParseResult>();
	auto expr = transformer.Transform<unique_ptr<ParsedExpression>>(list_pr.Child<ListParseResult>(0));
	auto is_test_opt = list_pr.Child<OptionalParseResult>(1);
	if (!is_test_opt.HasResult()) {
		return expr;
	}
	auto is_test_expr_repeat = is_test_opt.optional_result->Cast<RepeatParseResult>();
	for (auto &is_test_expr : is_test_expr_repeat.children) {
		throw NotImplementedException("IsTest has not yet been implemented.");
		auto expr_type = transformer.Transform<ExpressionType>(is_test_expr);
		vector<unique_ptr<ParsedExpression>> inner_list_children;
		inner_list_children.push_back(std::move(expr));
		expr = make_uniq<OperatorExpression>(expr_type, std::move(inner_list_children));
	}
	return expr;
}

// IsDistinctFromExpression <- ComparisonExpression (IsDistinctFromOp ComparisonExpression)*
unique_ptr<ParsedExpression> PEGTransformerFactory::TransformIsDistinctFromExpression(PEGTransformer &transformer,
                                                                        optional_ptr<ParseResult> parse_result) {
	auto &list_pr = parse_result->Cast<ListParseResult>();
	auto expr = transformer.Transform<unique_ptr<ParsedExpression>>(list_pr.Child<ListParseResult>(0));
	auto is_test_opt = list_pr.Child<OptionalParseResult>(1);
	if (!is_test_opt.HasResult()) {
		return expr;
	}
	throw NotImplementedException("IsDistinctFromOp has not yet been implemented");
}

// ComparisonExpression <- BetweenInLikeExpression (ComparisonOperator BetweenInLikeExpression)*
unique_ptr<ParsedExpression> PEGTransformerFactory::TransformComparisonExpression(PEGTransformer &transformer,
                                                                        optional_ptr<ParseResult> parse_result) {
	auto &list_pr = parse_result->Cast<ListParseResult>();
	auto expr = transformer.Transform<unique_ptr<ParsedExpression>>(list_pr.Child<ListParseResult>(0));
	auto comparison_opt = list_pr.Child<OptionalParseResult>(1);
	if (!comparison_opt.HasResult()) {
		return expr;
	}
	throw NotImplementedException("ComparisonOperator has not yet been implemented");
}

// BetweenInLikeExpression <- OtherOperatorExpression BetweenInLikeOp?
unique_ptr<ParsedExpression> PEGTransformerFactory::TransformBetweenInLikeExpression(PEGTransformer &transformer,
                                                                        optional_ptr<ParseResult> parse_result) {
	auto &list_pr = parse_result->Cast<ListParseResult>();
	auto expr = transformer.Transform<unique_ptr<ParsedExpression>>(list_pr.Child<ListParseResult>(0));
	auto comparison_opt = list_pr.Child<OptionalParseResult>(1);
	if (!comparison_opt.HasResult()) {
		return expr;
	}
	throw NotImplementedException("BetweenInLikeOp has not yet been implemented");
}

// OtherOperatorExpression <- BitwiseExpression (OtherOperator BitwiseExpression)*
unique_ptr<ParsedExpression> PEGTransformerFactory::TransformOtherOperatorExpression(PEGTransformer &transformer,
                                                                        optional_ptr<ParseResult> parse_result) {
	auto &list_pr = parse_result->Cast<ListParseResult>();
	auto expr = transformer.Transform<unique_ptr<ParsedExpression>>(list_pr.Child<ListParseResult>(0));
	auto other_operator_opt = list_pr.Child<OptionalParseResult>(1);
	if (!other_operator_opt.HasResult()) {
		return expr;
	}
	throw NotImplementedException("OtherOperator has not yet been implemented");
}

// BitwiseExpression <- AdditiveExpression (BitOperator AdditiveExpression)*
unique_ptr<ParsedExpression> PEGTransformerFactory::TransformBitwiseExpression(PEGTransformer &transformer,
                                                                        optional_ptr<ParseResult> parse_result) {
	auto &list_pr = parse_result->Cast<ListParseResult>();
	auto expr = transformer.Transform<unique_ptr<ParsedExpression>>(list_pr.Child<ListParseResult>(0));
	auto bit_operator_opt = list_pr.Child<OptionalParseResult>(1);
	if (!bit_operator_opt.HasResult()) {
		return expr;
	}
	throw NotImplementedException("BitOperator has not yet been implemented");
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

ExpressionType PEGTransformerFactory::TransformOperator(PEGTransformer &transformer,
                                                        optional_ptr<ParseResult> parse_result) {
	auto &list_pr = parse_result->Cast<ListParseResult>();
	auto &choice_pr = list_pr.Child<ChoiceParseResult>(0);
	if (choice_pr.result->type == ParseResultType::OPERATOR) {
		return OperatorToExpressionType(choice_pr.result->Cast<OperatorParseResult>().operator_token);
	}
	return transformer.Transform<ExpressionType>(choice_pr.result);
}

ExpressionType PEGTransformerFactory::TransformConjunctionOperator(PEGTransformer &transformer,
                                                                   optional_ptr<ParseResult> parse_result) {
	auto &list_pr = parse_result->Cast<ListParseResult>();
	return transformer.TransformEnum<ExpressionType>(list_pr.Child<ChoiceParseResult>(0).result);
}

ExpressionType PEGTransformerFactory::TransformIsOperator(PEGTransformer &transformer,
                                                          optional_ptr<ParseResult> parse_result) {
	auto &list_pr = parse_result->Cast<ListParseResult>();
	bool is_not = list_pr.Child<OptionalParseResult>(1).HasResult();
	bool is_distinct = list_pr.Child<OptionalParseResult>(2).HasResult();
	if (is_distinct && is_not) {
		return ExpressionType::COMPARE_NOT_DISTINCT_FROM;
	}
	if (is_distinct) {
		return ExpressionType::COMPARE_DISTINCT_FROM;
	}
	if (is_not) {
		return ExpressionType::OPERATOR_IS_NOT_NULL;
	}
	return ExpressionType::OPERATOR_IS_NULL;
}

ExpressionType PEGTransformerFactory::TransformInOperator(PEGTransformer &transformer,
                                                          optional_ptr<ParseResult> parse_result) {
	auto &list_pr = parse_result->Cast<ListParseResult>();
	auto is_not = list_pr.Child<OptionalParseResult>(0).HasResult();
	if (is_not) {
		return ExpressionType::COMPARE_NOT_IN;
	}
	return ExpressionType::COMPARE_IN;
}

ExpressionType PEGTransformerFactory::TransformLambdaOperator(PEGTransformer &transformer,
                                                              optional_ptr<ParseResult> parse_result) {
	return ExpressionType::LAMBDA;
}

ExpressionType PEGTransformerFactory::TransformBetweenOperator(PEGTransformer &transformer,
                                                               optional_ptr<ParseResult> parse_result) {
	auto &list_pr = parse_result->Cast<ListParseResult>();
	if (list_pr.Child<OptionalParseResult>(0).HasResult()) {
		return ExpressionType::COMPARE_NOT_BETWEEN;
	}
	return ExpressionType::COMPARE_BETWEEN;
}

unique_ptr<ParsedExpression> PEGTransformerFactory::TransformIndirection(PEGTransformer &transformer,
                                                                         optional_ptr<ParseResult> parse_result) {
	auto &list_pr = parse_result->Cast<ListParseResult>();
	return transformer.Transform<unique_ptr<ParsedExpression>>(list_pr.Child<ChoiceParseResult>(0).result);
}

unique_ptr<ParsedExpression> PEGTransformerFactory::TransformCastOperator(PEGTransformer &transformer,
                                                                          optional_ptr<ParseResult> parse_result) {
	auto &list_pr = parse_result->Cast<ListParseResult>();
	auto type = transformer.Transform<LogicalType>(list_pr.Child<ListParseResult>(1));
	// We input a dummy constant expression but replace this later with the real expression that precedes this post-fix
	// castOperator
	return make_uniq<CastExpression>(type, make_uniq<ConstantExpression>(Value()));
}

unique_ptr<ParsedExpression> PEGTransformerFactory::TransformDotOperator(PEGTransformer &transformer,
                                                                         optional_ptr<ParseResult> parse_result) {
	auto &list_pr = parse_result->Cast<ListParseResult>();
	auto nested_list = list_pr.Child<ListParseResult>(1);
	auto choice_pr = nested_list.Child<ChoiceParseResult>(0);
	if (choice_pr.name == "ColLabel") {
		return make_uniq<ConstantExpression>(transformer.Transform<string>(choice_pr.result));
	}
	if (choice_pr.name == "FunctionExpression") {
		throw NotImplementedException("Not implemented FunctionExpression in DotOperator");
		// return transformer.Transform<unique_ptr<ParsedExpression>>(choice_pr.result);
	}
	throw InternalException("Unexpected rule encountered in 'DotOperator'");
}

unique_ptr<ParsedExpression> PEGTransformerFactory::TransformSliceExpression(PEGTransformer &transformer,
                                                                             optional_ptr<ParseResult> parse_result) {
	auto &list_pr = parse_result->Cast<ListParseResult>();
	auto slice_bound = transformer.Transform<vector<unique_ptr<ParsedExpression>>>(list_pr.Child<ListParseResult>(1));
	if (slice_bound.size() == 1) {
		return make_uniq<OperatorExpression>(ExpressionType::ARRAY_EXTRACT, std::move(slice_bound));
	}
	return make_uniq<OperatorExpression>(ExpressionType::ARRAY_SLICE, std::move(slice_bound));
}

vector<unique_ptr<ParsedExpression>>
PEGTransformerFactory::TransformSliceBound(PEGTransformer &transformer, optional_ptr<ParseResult> parse_result) {
	auto &list_pr = parse_result->Cast<ListParseResult>();
	vector<unique_ptr<ParsedExpression>> slice_bounds;
	auto start_slice_opt = list_pr.Child<OptionalParseResult>(0);
	auto end_slice_opt = list_pr.Child<OptionalParseResult>(1);
	auto step_slice_opt = list_pr.Child<OptionalParseResult>(2);
	if (!end_slice_opt.HasResult() && !step_slice_opt.HasResult()) {
		if (start_slice_opt.HasResult()) {
			slice_bounds.push_back(
			    transformer.Transform<unique_ptr<ParsedExpression>>(start_slice_opt.optional_result));
		}
		return slice_bounds;
	}
	auto const_list = make_uniq<ConstantExpression>(Value::LIST(LogicalType::INTEGER, vector<Value>()));
	if (start_slice_opt.HasResult()) {
		slice_bounds.push_back(transformer.Transform<unique_ptr<ParsedExpression>>(start_slice_opt.optional_result));
	} else {
		slice_bounds.push_back(const_list->Copy());
	}
	if (end_slice_opt.HasResult()) {
		slice_bounds.push_back(transformer.Transform<unique_ptr<ParsedExpression>>(end_slice_opt.optional_result));
	} else {
		slice_bounds.push_back(const_list->Copy());
	}
	if (step_slice_opt.HasResult()) {
		slice_bounds.push_back(transformer.Transform<unique_ptr<ParsedExpression>>(step_slice_opt.optional_result));
	}
	return slice_bounds;
}

unique_ptr<ParsedExpression> PEGTransformerFactory::TransformEndSliceBound(PEGTransformer &transformer,
                                                                           optional_ptr<ParseResult> parse_result) {
	auto &list_pr = parse_result->Cast<ListParseResult>();
	auto nested_list_opt = list_pr.Child<OptionalParseResult>(1);
	// If either the lower or upper bound is not specified, we use an empty constant LIST,
	// which we handle in the execution.
	auto const_list = make_uniq<ConstantExpression>(Value::LIST(LogicalType::INTEGER, vector<Value>()));
	if (nested_list_opt.HasResult()) {
		auto nested_list = nested_list_opt.optional_result->Cast<ListParseResult>();
		auto choice_pr = nested_list.Child<ChoiceParseResult>(0);
		if (choice_pr.result->type == ParseResultType::KEYWORD) {
			// We have hit the '-'
			return const_list;
		}
		if (choice_pr.result->type == ParseResultType::LIST) {
			return transformer.Transform<unique_ptr<ParsedExpression>>(choice_pr.result);
		}
		throw InternalException("Unexpected parse result type encountered");
	}
	// return empty list here
	return const_list;
}

unique_ptr<ParsedExpression> PEGTransformerFactory::TransformStepSliceBound(PEGTransformer &transformer,
                                                                            optional_ptr<ParseResult> parse_result) {
	auto &list_pr = parse_result->Cast<ListParseResult>();
	auto expression_opt = list_pr.Child<OptionalParseResult>(1);
	if (expression_opt.HasResult()) {
		return transformer.Transform<unique_ptr<ParsedExpression>>(expression_opt.optional_result);
	}
	return make_uniq<ConstantExpression>(Value::LIST(LogicalType::INTEGER, vector<Value>()));
}

} // namespace duckdb
