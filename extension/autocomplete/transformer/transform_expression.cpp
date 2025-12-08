#include "duckdb/common/enums/date_part_specifier.hpp"
#include "transformer/peg_transformer.hpp"
#include "duckdb/parser/expression/comparison_expression.hpp"
#include "duckdb/parser/expression/between_expression.hpp"
#include "duckdb/parser/expression/operator_expression.hpp"
#include "duckdb/parser/expression/cast_expression.hpp"
#include "duckdb/parser/expression/conjunction_expression.hpp"

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
			function_expr->children.insert(function_expr->children.begin(), std::move(expr));
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
			auto repeat_list = child->Cast<ListParseResult>();
			column_names.push_back(repeat_list.Child<IdentifierParseResult>(0).identifier);
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

unique_ptr<ColumnRefExpression>
PEGTransformerFactory::TransformSchemaReservedTableColumnName(PEGTransformer &transformer,
                                                              optional_ptr<ParseResult> parse_result) {
	auto &list_pr = parse_result->Cast<ListParseResult>();
	vector<string> column_names;
	column_names.push_back(transformer.Transform<string>(list_pr.Child<ListParseResult>(0)));
	column_names.push_back(transformer.Transform<string>(list_pr.Child<ListParseResult>(1)));
	column_names.push_back(list_pr.Child<IdentifierParseResult>(2).identifier);
	return make_uniq<ColumnRefExpression>(std::move(column_names));
}

string PEGTransformerFactory::TransformReservedTableQualification(PEGTransformer &transformer,
                                                                  optional_ptr<ParseResult> parse_result) {
	auto &list_pr = parse_result->Cast<ListParseResult>();
	return list_pr.Child<IdentifierParseResult>(0).identifier;
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

QualifiedName PEGTransformerFactory::TransformSchemaReservedFunctionName(PEGTransformer &transformer,
                                                                         optional_ptr<ParseResult> parse_result) {
	auto &list_pr = parse_result->Cast<ListParseResult>();
	QualifiedName result;
	result.catalog = INVALID_CATALOG;
	result.schema = transformer.Transform<string>(list_pr.Child<ListParseResult>(0));
	result.name = list_pr.Child<IdentifierParseResult>(1).identifier;
	return result;
}

QualifiedName
PEGTransformerFactory::TransformCatalogReservedSchemaFunctionName(PEGTransformer &transformer,
                                                                  optional_ptr<ParseResult> parse_result) {
	auto &list_pr = parse_result->Cast<ListParseResult>();
	QualifiedName result;
	auto opt_schema = list_pr.Child<OptionalParseResult>(1);
	if (opt_schema.HasResult()) {
		result.catalog = transformer.Transform<string>(list_pr.Child<ListParseResult>(0));
		result.schema = transformer.Transform<string>(opt_schema.optional_result);
	} else {
		result.schema = transformer.Transform<string>(list_pr.Child<ListParseResult>(0));
	}
	result.name = list_pr.Child<IdentifierParseResult>(2).identifier;
	return result;
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

// Expression <- LambdaArrowExpression
unique_ptr<ParsedExpression> PEGTransformerFactory::TransformExpression(PEGTransformer &transformer,
                                                                        optional_ptr<ParseResult> parse_result) {
	auto &list_pr = parse_result->Cast<ListParseResult>();
	return transformer.Transform<unique_ptr<ParsedExpression>>(list_pr.Child<ListParseResult>(0));
}

unique_ptr<ParsedExpression>
PEGTransformerFactory::TransformLambdaArrowExpression(PEGTransformer &transformer,
                                                      optional_ptr<ParseResult> parse_result) {
	auto &list_pr = parse_result->Cast<ListParseResult>();
	auto expr = transformer.Transform<unique_ptr<ParsedExpression>>(list_pr.Child<ListParseResult>(0));
	auto lambda_opt = list_pr.Child<OptionalParseResult>(1);
	if (!lambda_opt.HasResult()) {
		return expr;
	}
	auto inner_lambda_list = lambda_opt.optional_result->Cast<RepeatParseResult>();
	for (auto lambda_expr : inner_lambda_list.children) {
		auto &inner_list_pr = lambda_expr->Cast<ListParseResult>();
		auto right_expr = transformer.Transform<unique_ptr<ParsedExpression>>(inner_list_pr.Child<ListParseResult>(1));
		expr = make_uniq<LambdaExpression>(std::move(expr), std::move(right_expr));
	}
	return expr;
}

// LogicalOrExpression <- LogicalAndExpression ('OR' LogicalAndExpression)*
unique_ptr<ParsedExpression>
PEGTransformerFactory::TransformLogicalOrExpression(PEGTransformer &transformer,
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
		expr = make_uniq<ConjunctionExpression>(ExpressionType::CONJUNCTION_OR, std::move(expr), std::move(right_expr));
	}
	return expr;
}

// LogicalAndExpression <- LogicalNotExpression ('AND' LogicalNotExpression)*
unique_ptr<ParsedExpression>
PEGTransformerFactory::TransformLogicalAndExpression(PEGTransformer &transformer,
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
		expr =
		    make_uniq<ConjunctionExpression>(ExpressionType::CONJUNCTION_AND, std::move(expr), std::move(right_expr));
	}
	return expr;
}

// LogicalNotExpression <- 'NOT'* IsExpression
unique_ptr<ParsedExpression>
PEGTransformerFactory::TransformLogicalNotExpression(PEGTransformer &transformer,
                                                     optional_ptr<ParseResult> parse_result) {
	auto &list_pr = parse_result->Cast<ListParseResult>();
	auto expr = transformer.Transform<unique_ptr<ParsedExpression>>(list_pr.Child<ListParseResult>(1));
	auto not_expr_opt = list_pr.Child<OptionalParseResult>(0);
	if (!not_expr_opt.HasResult()) {
		return expr;
	}
	auto not_expr_repeat = not_expr_opt.optional_result->Cast<RepeatParseResult>();
	size_t n = not_expr_repeat.children.size();
	for (size_t i = 0; i < n; i++) {
		vector<unique_ptr<ParsedExpression>> inner_list_children;
		inner_list_children.push_back(std::move(expr));
		expr = make_uniq<OperatorExpression>(ExpressionType::OPERATOR_NOT, std::move(inner_list_children));
	}
	return expr;
}

// IsExpression <- IsDistinctFromExpression IsTest*
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
		auto is_expr = transformer.Transform<unique_ptr<ParsedExpression>>(is_test_expr);
		if (is_expr->GetExpressionClass() == ExpressionClass::COMPARISON) {
			auto compare_expr = unique_ptr_cast<ParsedExpression, ComparisonExpression>(std::move(is_expr));
			compare_expr->left = std::move(expr);
			expr = std::move(compare_expr);
		} else if (is_expr->GetExpressionClass() == ExpressionClass::OPERATOR) {
			auto operator_expr = unique_ptr_cast<ParsedExpression, OperatorExpression>(std::move(is_expr));
			operator_expr->children.insert(operator_expr->children.begin(), std::move(expr));
			expr = std::move(operator_expr);
		} else {
			throw InternalException("Unexpected expression encountered in IsExpression: %s",
			                        ExpressionClassToString(is_expr->GetExpressionClass()));
		}
	}
	return expr;
}

unique_ptr<ParsedExpression> PEGTransformerFactory::TransformIsTest(PEGTransformer &transformer,
                                                                    optional_ptr<ParseResult> parse_result) {
	auto &list_pr = parse_result->Cast<ListParseResult>();
	return transformer.Transform<unique_ptr<ParsedExpression>>(list_pr.Child<ChoiceParseResult>(0).result);
}

unique_ptr<ParsedExpression> PEGTransformerFactory::TransformIsLiteral(PEGTransformer &transformer,
                                                                       optional_ptr<ParseResult> parse_result) {
	auto &list_pr = parse_result->Cast<ListParseResult>();
	auto not_expr = list_pr.Child<OptionalParseResult>(1);
	auto inner_list_pr = list_pr.Child<ListParseResult>(2);
	auto literal_value = transformer.TransformEnum<Value>(inner_list_pr.Child<ChoiceParseResult>(0).result);
	if (literal_value.IsNull()) {
		auto expr_type = not_expr.HasResult() ? ExpressionType::OPERATOR_IS_NOT_NULL : ExpressionType::OPERATOR_IS_NULL;
		return make_uniq<OperatorExpression>(expr_type, nullptr);
	}
	auto expr_type =
	    not_expr.HasResult() ? ExpressionType::COMPARE_NOT_DISTINCT_FROM : ExpressionType::COMPARE_DISTINCT_FROM;
	return make_uniq<ComparisonExpression>(expr_type, nullptr, make_uniq<ConstantExpression>(literal_value));
}

unique_ptr<ParsedExpression> PEGTransformerFactory::TransformIsNotNull(PEGTransformer &transformer,
                                                                       optional_ptr<ParseResult> parse_result) {
	return make_uniq<OperatorExpression>(ExpressionType::OPERATOR_IS_NOT_NULL, nullptr);
}

unique_ptr<ParsedExpression> PEGTransformerFactory::TransformIsNull(PEGTransformer &transformer,
                                                                    optional_ptr<ParseResult> parse_result) {
	return make_uniq<OperatorExpression>(ExpressionType::OPERATOR_IS_NULL, nullptr);
}

// IsDistinctFromExpression <- ComparisonExpression (IsDistinctFromOp ComparisonExpression)*
unique_ptr<ParsedExpression>
PEGTransformerFactory::TransformIsDistinctFromExpression(PEGTransformer &transformer,
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
unique_ptr<ParsedExpression>
PEGTransformerFactory::TransformComparisonExpression(PEGTransformer &transformer,
                                                     optional_ptr<ParseResult> parse_result) {
	auto &list_pr = parse_result->Cast<ListParseResult>();
	auto expr = transformer.Transform<unique_ptr<ParsedExpression>>(list_pr.Child<ListParseResult>(0));
	auto comparison_opt = list_pr.Child<OptionalParseResult>(1);
	if (!comparison_opt.HasResult()) {
		return expr;
	}
	auto comparison_repeat = comparison_opt.optional_result->Cast<RepeatParseResult>();
	for (auto &comparison_expr : comparison_repeat.children) {
		auto &inner_list_pr = comparison_expr->Cast<ListParseResult>();
		auto comparison_operator = transformer.Transform<ExpressionType>(inner_list_pr.Child<ListParseResult>(0));
		auto right_expr = transformer.Transform<unique_ptr<ParsedExpression>>(inner_list_pr.Child<ListParseResult>(1));
		expr = make_uniq<ComparisonExpression>(comparison_operator, std::move(expr), std::move(right_expr));
	}
	return expr;
}

ExpressionType PEGTransformerFactory::TransformComparisonOperator(PEGTransformer &transformer,
                                                                  optional_ptr<ParseResult> parse_result) {
	auto &list_pr = parse_result->Cast<ListParseResult>();
	return transformer.TransformEnum<ExpressionType>(list_pr.Child<ChoiceParseResult>(0).result);
}

// BetweenInLikeExpression <- OtherOperatorExpression BetweenInLikeOp?
unique_ptr<ParsedExpression>
PEGTransformerFactory::TransformBetweenInLikeExpression(PEGTransformer &transformer,
                                                        optional_ptr<ParseResult> parse_result) {
	auto &list_pr = parse_result->Cast<ListParseResult>();
	auto expr = transformer.Transform<unique_ptr<ParsedExpression>>(list_pr.Child<ListParseResult>(0));
	auto between_in_like_opt = list_pr.Child<OptionalParseResult>(1);
	if (!between_in_like_opt.HasResult()) {
		return expr;
	}
	auto between_in_like_expr =
	    transformer.Transform<unique_ptr<ParsedExpression>>(between_in_like_opt.optional_result);
	if (between_in_like_expr->GetExpressionClass() == ExpressionClass::BETWEEN) {
		auto between_expr = unique_ptr_cast<ParsedExpression, BetweenExpression>(std::move(between_in_like_expr));
		between_expr->input = std::move(expr);
		expr = std::move(between_expr);
	} else if (between_in_like_expr->GetExpressionClass() == ExpressionClass::FUNCTION) {
		auto func_expr = unique_ptr_cast<ParsedExpression, FunctionExpression>(std::move(between_in_like_expr));
		func_expr->children.insert(func_expr->children.begin(), std::move(expr));
		expr = std::move(func_expr);
	}
	return expr;
}

unique_ptr<ParsedExpression> PEGTransformerFactory::TransformBetweenInLikeOp(PEGTransformer &transformer,
                                                                             optional_ptr<ParseResult> parse_result) {
	auto &list_pr = parse_result->Cast<ListParseResult>();
	auto not_expr = list_pr.Child<OptionalParseResult>(0);
	auto inner_list = list_pr.Child<ListParseResult>(1);
	auto expr = transformer.Transform<unique_ptr<ParsedExpression>>(inner_list.Child<ChoiceParseResult>(0).result);
	if (!not_expr.HasResult()) {
		return expr;
	}
	if (expr->GetExpressionType() == ExpressionType::COMPARE_BETWEEN) {
		expr->type = ExpressionType::COMPARE_NOT_BETWEEN;
	} else {
		// TODO(Dtenwolde)
		throw NotImplementedException("Not in combination with in or like is not yet implemented.");
	}
	return expr;
}

unique_ptr<ParsedExpression> PEGTransformerFactory::TransformBetweenClause(PEGTransformer &transformer,
                                                                           optional_ptr<ParseResult> parse_result) {
	auto &list_pr = parse_result->Cast<ListParseResult>();
	auto lower = transformer.Transform<unique_ptr<ParsedExpression>>(list_pr.Child<ListParseResult>(1));
	auto higher = transformer.Transform<unique_ptr<ParsedExpression>>(list_pr.Child<ListParseResult>(3));
	auto result = make_uniq<BetweenExpression>(nullptr, std::move(lower), std::move(higher));
	return std::move(result);
}

unique_ptr<ParsedExpression> PEGTransformerFactory::TransformLikeClause(PEGTransformer &transformer,
                                                                        optional_ptr<ParseResult> parse_result) {
	auto &list_pr = parse_result->Cast<ListParseResult>();
	string like_variation = transformer.Transform<string>(list_pr.Child<ListParseResult>(0));
	vector<unique_ptr<ParsedExpression>> like_children;
	like_children.push_back(transformer.Transform<unique_ptr<ParsedExpression>>(list_pr.Child<ListParseResult>(1)));
	auto escape_opt = list_pr.Child<OptionalParseResult>(2);
	if (escape_opt.HasResult()) {
		throw NotImplementedException("Escape is not yet implemented.");
	}
	auto result = make_uniq<FunctionExpression>(like_variation, std::move(like_children));
	return std::move(result);
}

string PEGTransformerFactory::TransformLikeVariations(PEGTransformer &transformer,
                                                      optional_ptr<ParseResult> parse_result) {
	auto &list_pr = parse_result->Cast<ListParseResult>();
	return transformer.TransformEnum<string>(list_pr.Child<ChoiceParseResult>(0).result);
}

// OtherOperatorExpression <- BitwiseExpression (OtherOperator BitwiseExpression)*
unique_ptr<ParsedExpression>
PEGTransformerFactory::TransformOtherOperatorExpression(PEGTransformer &transformer,
                                                        optional_ptr<ParseResult> parse_result) {
	auto &list_pr = parse_result->Cast<ListParseResult>();
	auto expr = transformer.Transform<unique_ptr<ParsedExpression>>(list_pr.Child<ListParseResult>(0));
	auto other_operator_opt = list_pr.Child<OptionalParseResult>(1);
	if (!other_operator_opt.HasResult()) {
		return expr;
	}
	auto other_operator_repeat = other_operator_opt.optional_result->Cast<RepeatParseResult>();
	for (auto &other_operator_expr : other_operator_repeat.children) {
		auto &inner_list_pr = other_operator_expr->Cast<ListParseResult>();
		auto other_operator = transformer.Transform<string>(inner_list_pr.Child<ListParseResult>(0));
		auto right_expr = transformer.Transform<unique_ptr<ParsedExpression>>(inner_list_pr.Child<ListParseResult>(1));
		if (other_operator == "||" || other_operator == "^@") {
			vector<unique_ptr<ParsedExpression>> children_function;
			children_function.push_back(std::move(expr));
			children_function.push_back(std::move(right_expr));
			auto func_expr = make_uniq<FunctionExpression>(std::move(other_operator), std::move(children_function));
			func_expr->is_operator = true;
			expr = std::move(func_expr);
		} else {
			throw NotImplementedException("Other operator for %s is not implemented.", other_operator);
		}
	}
	return expr;
}

string PEGTransformerFactory::TransformOtherOperator(PEGTransformer &transformer,
                                                     optional_ptr<ParseResult> parse_result) {
	auto &list_pr = parse_result->Cast<ListParseResult>();
	return transformer.Transform<string>(list_pr.Child<ChoiceParseResult>(0).result);
}

string PEGTransformerFactory::TransformStringOperator(PEGTransformer &transformer,
                                                      optional_ptr<ParseResult> parse_result) {
	auto &list_pr = parse_result->Cast<ListParseResult>();
	auto choice_pr = list_pr.Child<ChoiceParseResult>(0).result;
	return choice_pr->Cast<KeywordParseResult>().keyword;
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

// AdditiveExpression <- MultiplicativeExpression (Term MultiplicativeExpression)*
unique_ptr<ParsedExpression>
PEGTransformerFactory::TransformAdditiveExpression(PEGTransformer &transformer,
                                                   optional_ptr<ParseResult> parse_result) {
	auto &list_pr = parse_result->Cast<ListParseResult>();
	auto expr = transformer.Transform<unique_ptr<ParsedExpression>>(list_pr.Child<ListParseResult>(0));
	auto term_opt = list_pr.Child<OptionalParseResult>(1);
	if (!term_opt.HasResult()) {
		return expr;
	}
	auto term_repeat = term_opt.optional_result->Cast<RepeatParseResult>();
	for (auto &term_expr : term_repeat.children) {
		auto &inner_list_pr = term_expr->Cast<ListParseResult>();
		auto term = transformer.Transform<string>(inner_list_pr.Child<ListParseResult>(0));
		vector<unique_ptr<ParsedExpression>> term_children;
		term_children.push_back(std::move(expr));
		term_children.push_back(
		    transformer.Transform<unique_ptr<ParsedExpression>>(inner_list_pr.Child<ListParseResult>(1)));
		auto func_expr = make_uniq<FunctionExpression>(std::move(term), std::move(term_children));
		func_expr->is_operator = true;
		expr = std::move(func_expr);
	}
	return expr;
}

string PEGTransformerFactory::TransformTerm(PEGTransformer &transformer, optional_ptr<ParseResult> parse_result) {
	auto &list_pr = parse_result->Cast<ListParseResult>();
	auto choice_pr = list_pr.Child<ChoiceParseResult>(0).result;
	return choice_pr->Cast<KeywordParseResult>().keyword;
}

// MultiplicativeExpression <- ExponentiationExpression (Factor ExponentiationExpression)*
unique_ptr<ParsedExpression>
PEGTransformerFactory::TransformMultiplicativeExpression(PEGTransformer &transformer,
                                                         optional_ptr<ParseResult> parse_result) {
	auto &list_pr = parse_result->Cast<ListParseResult>();
	auto expr = transformer.Transform<unique_ptr<ParsedExpression>>(list_pr.Child<ListParseResult>(0));
	auto factor_opt = list_pr.Child<OptionalParseResult>(1);
	if (!factor_opt.HasResult()) {
		return expr;
	}
	auto factor_repeat = factor_opt.optional_result->Cast<RepeatParseResult>();
	for (auto &factor_expr : factor_repeat.children) {
		auto &inner_list_pr = factor_expr->Cast<ListParseResult>();
		auto factor = transformer.Transform<string>(inner_list_pr.Child<ListParseResult>(0));
		vector<unique_ptr<ParsedExpression>> factor_children;
		factor_children.push_back(std::move(expr));
		factor_children.push_back(
		    transformer.Transform<unique_ptr<ParsedExpression>>(inner_list_pr.Child<ListParseResult>(1)));
		auto func_expr = make_uniq<FunctionExpression>(std::move(factor), std::move(factor_children));
		func_expr->is_operator = true;
		expr = std::move(func_expr);
	}
	return expr;
}

string PEGTransformerFactory::TransformFactor(PEGTransformer &transformer, optional_ptr<ParseResult> parse_result) {
	auto &list_pr = parse_result->Cast<ListParseResult>();
	auto choice_pr = list_pr.Child<ChoiceParseResult>(0).result;
	return choice_pr->Cast<KeywordParseResult>().keyword;
}

// ExponentiationExpression <- CollateExpression (ExponentOperator CollateExpression)*
unique_ptr<ParsedExpression>
PEGTransformerFactory::TransformExponentiationExpression(PEGTransformer &transformer,
                                                         optional_ptr<ParseResult> parse_result) {
	auto &list_pr = parse_result->Cast<ListParseResult>();
	auto expr = transformer.Transform<unique_ptr<ParsedExpression>>(list_pr.Child<ListParseResult>(0));
	auto exponent_opt = list_pr.Child<OptionalParseResult>(1);
	if (!exponent_opt.HasResult()) {
		return expr;
	}
	throw NotImplementedException("Exponent has not yet been implemented");
}

// CollateExpression <- AtTimeZoneExpression (CollateOperator AtTimeZoneExpression)*
unique_ptr<ParsedExpression> PEGTransformerFactory::TransformCollateExpression(PEGTransformer &transformer,
                                                                               optional_ptr<ParseResult> parse_result) {
	auto &list_pr = parse_result->Cast<ListParseResult>();
	auto expr = transformer.Transform<unique_ptr<ParsedExpression>>(list_pr.Child<ListParseResult>(0));
	auto collate_opt = list_pr.Child<OptionalParseResult>(1);
	if (!collate_opt.HasResult()) {
		return expr;
	}
	throw NotImplementedException("Collate has not yet been implemented");
}

// AtTimeZoneExpression <- PrefixExpression (AtTimeZoneOperator PrefixExpression)*
unique_ptr<ParsedExpression>
PEGTransformerFactory::TransformAtTimeZoneExpression(PEGTransformer &transformer,
                                                     optional_ptr<ParseResult> parse_result) {
	auto &list_pr = parse_result->Cast<ListParseResult>();
	auto expr = transformer.Transform<unique_ptr<ParsedExpression>>(list_pr.Child<ListParseResult>(0));
	auto at_time_zone_opt = list_pr.Child<OptionalParseResult>(1);
	if (!at_time_zone_opt.HasResult()) {
		return expr;
	}
	throw NotImplementedException("AT TIME ZONE has not yet been implemented");
}

// PrefixExpression <- PrefixOperator* BaseExpression
unique_ptr<ParsedExpression> PEGTransformerFactory::TransformPrefixExpression(PEGTransformer &transformer,
                                                                              optional_ptr<ParseResult> parse_result) {
	auto &list_pr = parse_result->Cast<ListParseResult>();
	auto expr = transformer.Transform<unique_ptr<ParsedExpression>>(list_pr.Child<ListParseResult>(1));
	auto prefix_opt = list_pr.Child<OptionalParseResult>(0);
	if (!prefix_opt.HasResult()) {
		return expr;
	}
	auto prefix_repeat = prefix_opt.optional_result->Cast<RepeatParseResult>();
	for (auto &prefix_expr : prefix_repeat.children) {
		auto prefix = transformer.Transform<string>(prefix_expr);
		vector<unique_ptr<ParsedExpression>> children;
		children.push_back(std::move(expr));
		expr = make_uniq<FunctionExpression>(prefix, std::move(children));
	}
	return expr;
}

string PEGTransformerFactory::TransformPrefixOperator(PEGTransformer &transformer,
                                                      optional_ptr<ParseResult> parse_result) {
	auto &list_pr = parse_result->Cast<ListParseResult>();
	auto choice_pr = list_pr.Child<ChoiceParseResult>(0);
	return transformer.TransformEnum<string>(choice_pr.result);
}

// LiteralExpression <- StringLiteral / NumberLiteral / 'NULL' / 'TRUE' / 'FALSE'
unique_ptr<ParsedExpression> PEGTransformerFactory::TransformLiteralExpression(PEGTransformer &transformer,
                                                                               optional_ptr<ParseResult> parse_result) {
	auto &list_pr = parse_result->Cast<ListParseResult>();
	auto &matched_rule_result = list_pr.Child<ChoiceParseResult>(0);
	if (matched_rule_result.name == "StringLiteral") {
		return make_uniq<ConstantExpression>(Value(transformer.Transform<string>(matched_rule_result.result)));
	}
	return transformer.Transform<unique_ptr<ParsedExpression>>(matched_rule_result.result);
}

// ParensExpression <- Parens(Expression)
unique_ptr<ParsedExpression> PEGTransformerFactory::TransformParensExpression(PEGTransformer &transformer,
                                                                              optional_ptr<ParseResult> parse_result) {
	auto &list_pr = parse_result->Cast<ListParseResult>();
	auto extract_parens = ExtractResultFromParens(list_pr.Child<ListParseResult>(0));
	return transformer.Transform<unique_ptr<ParsedExpression>>(extract_parens);
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
		return transformer.Transform<unique_ptr<ParsedExpression>>(choice_pr.result);
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
			return std::move(const_list);
		}
		if (choice_pr.result->type == ParseResultType::LIST) {
			return transformer.Transform<unique_ptr<ParsedExpression>>(choice_pr.result);
		}
		throw InternalException("Unexpected parse result type encountered");
	}
	// return empty list here
	return std::move(const_list);
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

unique_ptr<ColumnRefExpression>
PEGTransformerFactory::TransformTableReservedColumnName(PEGTransformer &transformer,
                                                        optional_ptr<ParseResult> parse_result) {
	auto &list_pr = parse_result->Cast<ListParseResult>();
	auto table = transformer.Transform<string>(list_pr.Child<ListParseResult>(0));
	auto column = list_pr.Child<IdentifierParseResult>(1).identifier;
	return make_uniq<ColumnRefExpression>(column, table);
	;
}

string PEGTransformerFactory::TransformTableQualification(PEGTransformer &transformer,
                                                          optional_ptr<ParseResult> parse_result) {
	auto &list_pr = parse_result->Cast<ListParseResult>();
	return list_pr.Child<IdentifierParseResult>(0).identifier;
}

unique_ptr<ParsedExpression> PEGTransformerFactory::TransformStarExpression(PEGTransformer &transformer,
                                                                            optional_ptr<ParseResult> parse_result) {
	auto &list_pr = parse_result->Cast<ListParseResult>();

	auto result = make_uniq<StarExpression>();
	auto repeat_colid_opt = list_pr.Child<OptionalParseResult>(0);
	if (repeat_colid_opt.HasResult()) {
		auto repeat_colid = repeat_colid_opt.optional_result->Cast<RepeatParseResult>();
	}
	transformer.TransformOptional<qualified_column_set_t>(list_pr, 2, result->exclude_list);
	auto replace_list_opt = list_pr.Child<OptionalParseResult>(3);
	if (replace_list_opt.HasResult()) {
		result->replace_list = transformer.Transform<case_insensitive_map_t<unique_ptr<ParsedExpression>>>(
		    replace_list_opt.optional_result);
	}
	auto rename_list_opt = list_pr.Child<OptionalParseResult>(4);
	if (rename_list_opt.HasResult()) {
		result->rename_list = transformer.Transform<qualified_column_map_t<string>>(rename_list_opt.optional_result);
	}
	return std::move(result);
}

unique_ptr<WindowExpression> PEGTransformerFactory::TransformOverClause(PEGTransformer &transformer,
                                                                        optional_ptr<ParseResult> parse_result) {
	auto &list_pr = parse_result->Cast<ListParseResult>();
	return transformer.Transform<unique_ptr<WindowExpression>>(list_pr.Child<ListParseResult>(1));
}

unique_ptr<WindowExpression> PEGTransformerFactory::TransformWindowFrame(PEGTransformer &transformer,
                                                                         optional_ptr<ParseResult> parse_result) {
	auto &list_pr = parse_result->Cast<ListParseResult>();
	auto choice_pr = list_pr.Child<ChoiceParseResult>(0);
	if (choice_pr.result->type == ParseResultType::IDENTIFIER) {
		throw NotImplementedException("Identifier in Window Function has not yet been implemented");
	}
	return transformer.Transform<unique_ptr<WindowExpression>>(choice_pr.result);
}

unique_ptr<WindowExpression>
PEGTransformerFactory::TransformWindowFrameDefinition(PEGTransformer &transformer,
                                                      optional_ptr<ParseResult> parse_result) {
	auto &list_pr = parse_result->Cast<ListParseResult>();
	return transformer.Transform<unique_ptr<WindowExpression>>(list_pr.Child<ChoiceParseResult>(0).result);
}

unique_ptr<WindowExpression>
PEGTransformerFactory::TransformWindowFrameContentsParens(PEGTransformer &transformer,
                                                          optional_ptr<ParseResult> parse_result) {
	auto &list_pr = parse_result->Cast<ListParseResult>();
	auto extract_parens = ExtractResultFromParens(list_pr.Child<ListParseResult>(0));
	return transformer.Transform<unique_ptr<WindowExpression>>(extract_parens);
}

unique_ptr<WindowExpression>
PEGTransformerFactory::TransformWindowFrameNameContentsParens(PEGTransformer &transformer,
                                                              optional_ptr<ParseResult> parse_result) {
	auto &list_pr = parse_result->Cast<ListParseResult>();
	auto extract_parens = ExtractResultFromParens(list_pr.Child<ListParseResult>(0))->Cast<ListParseResult>();
	auto window_name_opt = extract_parens.Child<OptionalParseResult>(0);
	if (window_name_opt.HasResult()) {
		throw NotImplementedException("Window name has not yet been implemented");
	}
	// TODO(Dtenwolde) Use the window name
	auto window_frame_contents =
	    transformer.Transform<unique_ptr<WindowExpression>>(extract_parens.Child<ListParseResult>(1));
	return window_frame_contents;
}

unique_ptr<WindowExpression>
PEGTransformerFactory::TransformWindowFrameContents(PEGTransformer &transformer,
                                                    optional_ptr<ParseResult> parse_result) {
	auto &list_pr = parse_result->Cast<ListParseResult>();
	//! Create a dummy result to add modifiers to
	auto result =
	    make_uniq<WindowExpression>(ExpressionType::WINDOW_AGGREGATE, INVALID_CATALOG, INVALID_SCHEMA, string());
	auto partition_opt = list_pr.Child<OptionalParseResult>(0);
	if (partition_opt.HasResult()) {
		result->partitions = transformer.Transform<vector<unique_ptr<ParsedExpression>>>(partition_opt.optional_result);
	}
	auto order_by_opt = list_pr.Child<OptionalParseResult>(1);
	if (order_by_opt.HasResult()) {
		result->orders = transformer.Transform<vector<OrderByNode>>(order_by_opt.optional_result);
	}
	auto frame_opt = list_pr.Child<OptionalParseResult>(2);
	if (frame_opt.HasResult()) {
		throw NotImplementedException("Frame has not yet been implemented");
	} else {
		result->start = WindowBoundary::UNBOUNDED_PRECEDING;
		result->end = WindowBoundary::CURRENT_ROW_RANGE;
	}
	return result;
}

vector<unique_ptr<ParsedExpression>>
PEGTransformerFactory::TransformWindowPartition(PEGTransformer &transformer, optional_ptr<ParseResult> parse_result) {
	auto &list_pr = parse_result->Cast<ListParseResult>();
	auto expression_list = ExtractParseResultsFromList(list_pr.Child<ListParseResult>(2));
	vector<unique_ptr<ParsedExpression>> result;
	for (auto expression : expression_list) {
		result.push_back(transformer.Transform<unique_ptr<ParsedExpression>>(expression));
	}
	return result;
}

unique_ptr<ParsedExpression>
PEGTransformerFactory::TransformSpecialFunctionExpression(PEGTransformer &transformer,
                                                          optional_ptr<ParseResult> parse_result) {
	auto &list_pr = parse_result->Cast<ListParseResult>();
	return transformer.Transform<unique_ptr<ParsedExpression>>(list_pr.Child<ChoiceParseResult>(0).result);
}

unique_ptr<ParsedExpression>
PEGTransformerFactory::TransformCoalesceExpression(PEGTransformer &transformer,
                                                   optional_ptr<ParseResult> parse_result) {
	auto &list_pr = parse_result->Cast<ListParseResult>();
	auto result = make_uniq<OperatorExpression>(ExpressionType::OPERATOR_COALESCE);
	auto extract_parens = ExtractResultFromParens(list_pr.Child<ListParseResult>(1));
	auto expr_list = ExtractParseResultsFromList(extract_parens);
	for (auto expr : expr_list) {
		result->children.push_back(transformer.Transform<unique_ptr<ParsedExpression>>(expr));
	}
	return std::move(result);
}

unique_ptr<ParsedExpression> PEGTransformerFactory::TransformUnpackExpression(PEGTransformer &transformer,
                                                                              optional_ptr<ParseResult> parse_result) {
	auto &list_pr = parse_result->Cast<ListParseResult>();
	auto extract_parens = ExtractResultFromParens(list_pr.Child<ListParseResult>(1));
	auto result = make_uniq<OperatorExpression>(ExpressionType::OPERATOR_UNPACK);
	result->children.push_back(transformer.Transform<unique_ptr<ParsedExpression>>(extract_parens));
	return std::move(result);
}

unique_ptr<ParsedExpression> PEGTransformerFactory::TransformColumnsExpression(PEGTransformer &transformer,
                                                                               optional_ptr<ParseResult> parse_result) {
	auto &list_pr = parse_result->Cast<ListParseResult>();

	auto result = make_uniq<StarExpression>();
	auto expr =
	    transformer.Transform<unique_ptr<ParsedExpression>>(ExtractResultFromParens(list_pr.Child<ListParseResult>(2)));
	if (expr->GetExpressionType() == ExpressionType::STAR) {
		result = unique_ptr_cast<ParsedExpression, StarExpression>(std::move(expr));
	} else {
		result->expr = std::move(expr);
	}
	result->columns = true;
	return std::move(result);
}

unique_ptr<ParsedExpression> PEGTransformerFactory::TransformExtractExpression(PEGTransformer &transformer,
                                                                               optional_ptr<ParseResult> parse_result) {
	auto &list_pr = parse_result->Cast<ListParseResult>();
	auto extract_expressions = ExtractResultFromParens(list_pr.Child<ListParseResult>(1))->Cast<ListParseResult>();
	vector<unique_ptr<ParsedExpression>> expr_children;
	expr_children.push_back(
	    transformer.Transform<unique_ptr<ParsedExpression>>(extract_expressions.Child<ListParseResult>(0)));
	expr_children.push_back(
	    transformer.Transform<unique_ptr<ParsedExpression>>(extract_expressions.Child<ListParseResult>(2)));
	return make_uniq<FunctionExpression>(INVALID_CATALOG, "main", "date_part", std::move(expr_children));
}

unique_ptr<ParsedExpression> PEGTransformerFactory::TransformExtractArgument(PEGTransformer &transformer,
                                                                             optional_ptr<ParseResult> parse_result) {
	auto &list_pr = parse_result->Cast<ListParseResult>();
	auto choice_pr = list_pr.Child<ChoiceParseResult>(0).result;
	if (choice_pr->type == ParseResultType::IDENTIFIER) {
		return make_uniq<ConstantExpression>(Value(choice_pr->Cast<IdentifierParseResult>().identifier));
	}
	if (choice_pr->type == ParseResultType::STRING) {
		return make_uniq<ConstantExpression>(Value(choice_pr->Cast<StringLiteralParseResult>().result));
	}
	auto date_part = transformer.TransformEnum<DatePartSpecifier>(choice_pr);
	return make_uniq<ConstantExpression>(EnumUtil::ToString(date_part));
}

unique_ptr<ParsedExpression> PEGTransformerFactory::TransformLambdaExpression(PEGTransformer &transformer,
                                                                              optional_ptr<ParseResult> parse_result) {
	auto &list_pr = parse_result->Cast<ListParseResult>();

	auto col_id_list = ExtractParseResultsFromList(list_pr.Child<ListParseResult>(1));
	vector<string> parameters;
	for (auto colid : col_id_list) {
		parameters.push_back(transformer.Transform<string>(colid));
	}
	auto rhs_expr = transformer.Transform<unique_ptr<ParsedExpression>>(list_pr.Child<ListParseResult>(3));
	auto result = make_uniq<LambdaExpression>(parameters, std::move(rhs_expr));
	return std::move(result);
}

unique_ptr<ParsedExpression> PEGTransformerFactory::TransformNullIfExpression(PEGTransformer &transformer,
                                                                              optional_ptr<ParseResult> parse_result) {
	auto &list_pr = parse_result->Cast<ListParseResult>();
	auto extract_parens = ExtractResultFromParens(list_pr.Child<ListParseResult>(1));
	auto nested_list = extract_parens->Cast<ListParseResult>();
	vector<unique_ptr<ParsedExpression>> expr_children;
	expr_children.push_back(transformer.Transform<unique_ptr<ParsedExpression>>(nested_list.Child<ListParseResult>(0)));
	expr_children.push_back(transformer.Transform<unique_ptr<ParsedExpression>>(nested_list.Child<ListParseResult>(2)));
	return make_uniq<FunctionExpression>("nullif", std::move(expr_children));
}

unique_ptr<ParsedExpression> PEGTransformerFactory::TransformRowExpression(PEGTransformer &transformer,
                                                                           optional_ptr<ParseResult> parse_result) {
	auto &list_pr = parse_result->Cast<ListParseResult>();
	auto extract_parens = ExtractResultFromParens(list_pr.Child<ListParseResult>(1));
	auto expr_list = ExtractParseResultsFromList(extract_parens);
	vector<unique_ptr<ParsedExpression>> results;
	for (auto expr : expr_list) {
		results.push_back(transformer.Transform<unique_ptr<ParsedExpression>>(expr));
	}
	auto func_expr = make_uniq<FunctionExpression>("row", std::move(results));
	return std::move(func_expr);
}

unique_ptr<ParsedExpression> PEGTransformerFactory::TransformTrimExpression(PEGTransformer &transformer,
                                                                            optional_ptr<ParseResult> parse_result) {
	auto &list_pr = parse_result->Cast<ListParseResult>();
	auto extract_parens = ExtractResultFromParens(list_pr.Child<ListParseResult>(1));
	auto inner_list = extract_parens->Cast<ListParseResult>();
	string function_name = "trim";
	transformer.TransformOptional<string>(inner_list, 0, function_name);
	vector<unique_ptr<ParsedExpression>> trim_expressions;
	auto expr_list = ExtractParseResultsFromList(inner_list.Child<ListParseResult>(2));
	for (auto expr : expr_list) {
		trim_expressions.push_back(transformer.Transform<unique_ptr<ParsedExpression>>(expr));
	}
	auto trim_source_opt = inner_list.Child<OptionalParseResult>(1);
	if (trim_source_opt.HasResult()) {
		auto trim_source_expr = transformer.Transform<unique_ptr<ParsedExpression>>(trim_source_opt.optional_result);
		if (trim_source_expr) {
			trim_expressions.push_back(std::move(trim_source_expr));
		}
	}
	return make_uniq<FunctionExpression>(INVALID_CATALOG, DEFAULT_SCHEMA, function_name, std::move(trim_expressions));
}

string PEGTransformerFactory::TransformTrimDirection(PEGTransformer &transformer,
                                                     optional_ptr<ParseResult> parse_result) {
	auto &list_pr = parse_result->Cast<ListParseResult>();
	return transformer.TransformEnum<string>(list_pr.Child<ChoiceParseResult>(0).result);
}

unique_ptr<ParsedExpression> PEGTransformerFactory::TransformTrimSource(PEGTransformer &transformer,
                                                                        optional_ptr<ParseResult> parse_result) {
	auto &list_pr = parse_result->Cast<ListParseResult>();
	auto expr_opt = list_pr.Child<OptionalParseResult>(0);
	if (expr_opt.HasResult()) {
		return transformer.Transform<unique_ptr<ParsedExpression>>(expr_opt.optional_result);
	}
	return nullptr;
}

unique_ptr<ParsedExpression>
PEGTransformerFactory::TransformPositionExpression(PEGTransformer &transformer,
                                                   optional_ptr<ParseResult> parse_result) {
	auto &list_pr = parse_result->Cast<ListParseResult>();
	auto extract_parens = ExtractResultFromParens(list_pr.Child<ListParseResult>(1));
	auto position_values = extract_parens->Cast<ListParseResult>();
	vector<unique_ptr<ParsedExpression>> results;
	//! search_string IN string
	results.push_back(transformer.Transform<unique_ptr<ParsedExpression>>(position_values.Child<ListParseResult>(2)));
	results.push_back(transformer.Transform<unique_ptr<ParsedExpression>>(position_values.Child<ListParseResult>(0)));
	return make_uniq<FunctionExpression>("position", std::move(results));
}

unique_ptr<ParsedExpression> PEGTransformerFactory::TransformCastExpression(PEGTransformer &transformer,
                                                                            optional_ptr<ParseResult> parse_result) {
	auto &list_pr = parse_result->Cast<ListParseResult>();
	bool try_cast = transformer.Transform<bool>(list_pr.Child<ListParseResult>(0));
	auto extract_parens = ExtractResultFromParens(list_pr.Child<ListParseResult>(1))->Cast<ListParseResult>();
	auto expr = transformer.Transform<unique_ptr<ParsedExpression>>(extract_parens.Child<ListParseResult>(0));
	auto type = transformer.Transform<LogicalType>(extract_parens.Child<ListParseResult>(2));
	return make_uniq<CastExpression>(type, std::move(expr), try_cast);
}

bool PEGTransformerFactory::TransformCastOrTryCast(PEGTransformer &transformer,
                                                   optional_ptr<ParseResult> parse_result) {
	auto &list_pr = parse_result->Cast<ListParseResult>();
	auto choice_pr = list_pr.Child<ChoiceParseResult>(0);
	return StringUtil::Lower(choice_pr.result->Cast<KeywordParseResult>().keyword) == "try_cast";
}

unique_ptr<ParsedExpression> PEGTransformerFactory::TransformCaseExpression(PEGTransformer &transformer,
                                                                            optional_ptr<ParseResult> parse_result) {
	auto &list_pr = parse_result->Cast<ListParseResult>();
	auto result = make_uniq<CaseExpression>();
	unique_ptr<ParsedExpression> opt_expr;
	transformer.TransformOptional<unique_ptr<ParsedExpression>>(list_pr, 1, opt_expr);
	auto else_expr_opt = list_pr.Child<OptionalParseResult>(3);
	if (else_expr_opt.HasResult()) {
		result->else_expr = transformer.Transform<unique_ptr<ParsedExpression>>(else_expr_opt.optional_result);
	} else {
		result->else_expr = make_uniq<ConstantExpression>(Value());
	}

	auto cases_pr = list_pr.Child<RepeatParseResult>(2).children;
	for (auto &case_pr : cases_pr) {
		auto case_expr = transformer.Transform<CaseCheck>(case_pr);
		CaseCheck new_case;
		if (opt_expr) {
			new_case.when_expr = make_uniq<ComparisonExpression>(ExpressionType::COMPARE_EQUAL, opt_expr->Copy(),
			                                                     std::move(case_expr.when_expr));
		} else {
			new_case.when_expr = std::move(case_expr.when_expr);
		}
		new_case.then_expr = std::move(case_expr.then_expr);
		result->case_checks.push_back(std::move(new_case));
	}
	return std::move(result);
}

unique_ptr<ParsedExpression> PEGTransformerFactory::TransformCaseElse(PEGTransformer &transformer,
                                                                      optional_ptr<ParseResult> parse_result) {
	auto &list_pr = parse_result->Cast<ListParseResult>();
	return transformer.Transform<unique_ptr<ParsedExpression>>(list_pr.Child<ListParseResult>(1));
}

CaseCheck PEGTransformerFactory::TransformCaseWhenThen(PEGTransformer &transformer,
                                                       optional_ptr<ParseResult> parse_result) {
	auto &list_pr = parse_result->Cast<ListParseResult>();
	CaseCheck result;
	result.when_expr = transformer.Transform<unique_ptr<ParsedExpression>>(list_pr.Child<ListParseResult>(1));
	result.then_expr = transformer.Transform<unique_ptr<ParsedExpression>>(list_pr.Child<ListParseResult>(3));
	return result;
}

unique_ptr<ParsedExpression> PEGTransformerFactory::TransformTypeLiteral(PEGTransformer &transformer,
                                                                         optional_ptr<ParseResult> parse_result) {
	auto &list_pr = parse_result->Cast<ListParseResult>();
	auto colid = transformer.Transform<string>(list_pr.Child<ListParseResult>(0));
	auto type = LogicalType(TransformStringToLogicalTypeId(colid));
	if (type == LogicalTypeId::USER) {
		type = LogicalType::USER(colid);
	}
	auto string_literal = list_pr.Child<StringLiteralParseResult>(1).result;
	auto child = make_uniq<ConstantExpression>(Value(string_literal));
	auto result = make_uniq<CastExpression>(type, std::move(child));
	return std::move(result);
}

} // namespace duckdb
