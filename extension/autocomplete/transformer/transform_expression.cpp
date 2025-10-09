#include "transformer/peg_transformer.hpp"
#include "duckdb/parser/expression/comparison_expression.hpp"
#include "duckdb/parser/expression/between_expression.hpp"
#include "duckdb/parser/expression/operator_expression.hpp"
#include "duckdb/parser/expression/cast_expression.hpp"

namespace duckdb {

// // AnyAllOperator <- ComparisonOperator AnyOrAll
// unique_ptr<SQLStatement> PEGTransformerFactory::TransformAnyAllOperator(PEGTransformer &transformer,
//                                                                         optional_ptr<ParseResult> parse_result) {
// 	auto &list_pr = parse_result->Cast<ListParseResult>();
// 	throw NotImplementedException("Rule 'AnyAllOperator' has not been implemented yet");
// }
//
// // AnyOrAll <- 'ANY' / 'ALL'
// unique_ptr<SQLStatement> PEGTransformerFactory::TransformAnyOrAll(PEGTransformer &transformer,
//                                                                   optional_ptr<ParseResult> parse_result) {
// 	auto &list_pr = parse_result->Cast<ListParseResult>();
// 	throw NotImplementedException("Rule 'AnyOrAll' has not been implemented yet");
// }
//
// // AtTimeZoneOperator <- 'AT' 'TIME' 'ZONE'
// unique_ptr<SQLStatement> PEGTransformerFactory::TransformAtTimeZoneOperator(PEGTransformer &transformer,
//                                                                             optional_ptr<ParseResult> parse_result) {
// 	auto &list_pr = parse_result->Cast<ListParseResult>();
// 	throw NotImplementedException("Rule 'AtTimeZoneOperator' has not been implemented yet");
// }

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

// // BaseWindowName <- Identifier
// unique_ptr<SQLStatement> PEGTransformerFactory::TransformBaseWindowName(PEGTransformer &transformer,
//                                                                         optional_ptr<ParseResult> parse_result) {
// 	auto &list_pr = parse_result->Cast<ListParseResult>();
// 	throw NotImplementedException("Rule 'BaseWindowName' has not been implemented yet");
// }
//
// // BetweenOperator <- 'NOT'? 'BETWEEN'
// unique_ptr<SQLStatement> PEGTransformerFactory::TransformBetweenOperator(PEGTransformer &transformer,
//                                                                          optional_ptr<ParseResult> parse_result) {
// 	auto &list_pr = parse_result->Cast<ListParseResult>();
// 	throw NotImplementedException("Rule 'BetweenOperator' has not been implemented yet");
// }
//
// // BoundedListExpression <- '[' List(Expression)? ']'
// unique_ptr<SQLStatement> PEGTransformerFactory::TransformBoundedListExpression(PEGTransformer &transformer,
//                                                                                optional_ptr<ParseResult>
//                                                                                parse_result) {
// 	auto &list_pr = parse_result->Cast<ListParseResult>();
// 	throw NotImplementedException("Rule 'BoundedListExpression' has not been implemented yet");
// }
//
// // CaseElse <- 'ELSE' Expression
// unique_ptr<SQLStatement> PEGTransformerFactory::TransformCaseElse(PEGTransformer &transformer,
//                                                                   optional_ptr<ParseResult> parse_result) {
// 	auto &list_pr = parse_result->Cast<ListParseResult>();
// 	throw NotImplementedException("Rule 'CaseElse' has not been implemented yet");
// }
//
// // CaseExpression <- 'CASE' Expression? CaseWhenThen CaseWhenThen* CaseElse? 'END'
// unique_ptr<SQLStatement> PEGTransformerFactory::TransformCaseExpression(PEGTransformer &transformer,
//                                                                         optional_ptr<ParseResult> parse_result) {
// 	auto &list_pr = parse_result->Cast<ListParseResult>();
// 	throw NotImplementedException("Rule 'CaseExpression' has not been implemented yet");
// }
//
// // CaseWhenThen <- 'WHEN' Expression 'THEN' Expression
// unique_ptr<SQLStatement> PEGTransformerFactory::TransformCaseWhenThen(PEGTransformer &transformer,
//                                                                       optional_ptr<ParseResult> parse_result) {
// 	auto &list_pr = parse_result->Cast<ListParseResult>();
// 	throw NotImplementedException("Rule 'CaseWhenThen' has not been implemented yet");
// }
//
// // CastExpression <- CastOrTryCast Parens(Expression 'AS' Type)
// unique_ptr<SQLStatement> PEGTransformerFactory::TransformCastExpression(PEGTransformer &transformer,
//                                                                         optional_ptr<ParseResult> parse_result) {
// 	auto &list_pr = parse_result->Cast<ListParseResult>();
// 	throw NotImplementedException("Rule 'CastExpression' has not been implemented yet");
// }
//
// // CastOperator <- '::' Type
// unique_ptr<SQLStatement> PEGTransformerFactory::TransformCastOperator(PEGTransformer &transformer,
//                                                                       optional_ptr<ParseResult> parse_result) {
// 	auto &list_pr = parse_result->Cast<ListParseResult>();
// 	throw NotImplementedException("Rule 'CastOperator' has not been implemented yet");
// }
//
// // CastOrTryCast <- 'CAST' / 'TRY_CAST'
// unique_ptr<SQLStatement> PEGTransformerFactory::TransformCastOrTryCast(PEGTransformer &transformer,
//                                                                        optional_ptr<ParseResult> parse_result) {
// 	auto &list_pr = parse_result->Cast<ListParseResult>();
// 	throw NotImplementedException("Rule 'CastOrTryCast' has not been implemented yet");
// }
//
// // CatalogReservedSchemaFunctionName <- CatalogQualification ReservedSchemaQualification? ReservedFunctionName
// unique_ptr<SQLStatement>
// PEGTransformerFactory::TransformCatalogReservedSchemaFunctionName(PEGTransformer &transformer,
//                                                                   optional_ptr<ParseResult> parse_result) {
// 	auto &list_pr = parse_result->Cast<ListParseResult>();
// 	throw NotImplementedException("Rule 'CatalogReservedSchemaFunctionName' has not been implemented yet");
// }
//
// // CatalogReservedSchemaTableColumnName <- CatalogQualification ReservedSchemaQualification
// ReservedTableQualification
// // ReservedColumnName
// unique_ptr<SQLStatement>
// PEGTransformerFactory::TransformCatalogReservedSchemaTableColumnName(PEGTransformer &transformer,
//                                                                      optional_ptr<ParseResult> parse_result) {
// 	auto &list_pr = parse_result->Cast<ListParseResult>();
// 	throw NotImplementedException("Rule 'CatalogReservedSchemaTableColumnName' has not been implemented yet");
// }
//
// // CoalesceExpression <- 'COALESCE' Parens(List(Expression))
// unique_ptr<SQLStatement> PEGTransformerFactory::TransformCoalesceExpression(PEGTransformer &transformer,
//                                                                             optional_ptr<ParseResult> parse_result) {
// 	auto &list_pr = parse_result->Cast<ListParseResult>();
// 	throw NotImplementedException("Rule 'CoalesceExpression' has not been implemented yet");
// }
//
// // ColLabelParameter <- '$' ColLabel
// unique_ptr<SQLStatement> PEGTransformerFactory::TransformColLabelParameter(PEGTransformer &transformer,
//                                                                            optional_ptr<ParseResult> parse_result) {
// 	auto &list_pr = parse_result->Cast<ListParseResult>();
// 	throw NotImplementedException("Rule 'ColLabelParameter' has not been implemented yet");
// }
//
// // CollateOperator <- 'COLLATE'
// unique_ptr<SQLStatement> PEGTransformerFactory::TransformCollateOperator(PEGTransformer &transformer,
//                                                                          optional_ptr<ParseResult> parse_result) {
// 	auto &list_pr = parse_result->Cast<ListParseResult>();
// 	throw NotImplementedException("Rule 'CollateOperator' has not been implemented yet");
// }
//
// // ColumnReference <- CatalogReservedSchemaTableColumnName / SchemaReservedTableColumnName / TableReservedColumnName

// ColumnName
unique_ptr<ParsedExpression> PEGTransformerFactory::TransformColumnReference(PEGTransformer &transformer,
																			 optional_ptr<ParseResult> parse_result) {
	auto &list_pr = parse_result->Cast<ListParseResult>();
	auto identifiers = transformer.Transform<vector<string>>(list_pr.Child<ListParseResult>(0));
	return make_uniq<ColumnRefExpression>(std::move(identifiers));
}

// // ColumnsExpression <- '*'? 'COLUMNS' Parens(Expression)
// unique_ptr<SQLStatement> PEGTransformerFactory::TransformColumnsExpression(PEGTransformer &transformer,
//                                                                            optional_ptr<ParseResult> parse_result) {
// 	auto &list_pr = parse_result->Cast<ListParseResult>();
// 	throw NotImplementedException("Rule 'ColumnsExpression' has not been implemented yet");
// }
//
// // ComparisonOperator <- '=' / '<=' / '>=' / '<' / '>' / '<>' / '!=' / '=='
// unique_ptr<SQLStatement> PEGTransformerFactory::TransformComparisonOperator(PEGTransformer &transformer,
//                                                                             optional_ptr<ParseResult> parse_result) {
// 	auto &list_pr = parse_result->Cast<ListParseResult>();
// 	throw NotImplementedException("Rule 'ComparisonOperator' has not been implemented yet");
// }
//
// // ConjunctionOperator <- 'OR' / 'AND'
// unique_ptr<SQLStatement> PEGTransformerFactory::TransformConjunctionOperator(PEGTransformer &transformer,
//                                                                              optional_ptr<ParseResult> parse_result)
//                                                                              {
// 	auto &list_pr = parse_result->Cast<ListParseResult>();
// 	throw NotImplementedException("Rule 'ConjunctionOperator' has not been implemented yet");
// }
//
// // DefaultExpression <- 'DEFAULT'
// unique_ptr<SQLStatement> PEGTransformerFactory::TransformDefaultExpression(PEGTransformer &transformer,
//                                                                            optional_ptr<ParseResult> parse_result) {
// 	auto &list_pr = parse_result->Cast<ListParseResult>();
// 	throw NotImplementedException("Rule 'DefaultExpression' has not been implemented yet");
// }
//
// // DistinctFrom <- 'DISTINCT' 'FROM'
// unique_ptr<SQLStatement> PEGTransformerFactory::TransformDistinctFrom(PEGTransformer &transformer,
//                                                                       optional_ptr<ParseResult> parse_result) {
// 	auto &list_pr = parse_result->Cast<ListParseResult>();
// 	throw NotImplementedException("Rule 'DistinctFrom' has not been implemented yet");
// }
//
// // DistinctOrAll <- 'DISTINCT' / 'ALL'
// unique_ptr<SQLStatement> PEGTransformerFactory::TransformDistinctOrAll(PEGTransformer &transformer,
//                                                                        optional_ptr<ParseResult> parse_result) {
// 	auto &list_pr = parse_result->Cast<ListParseResult>();
// 	throw NotImplementedException("Rule 'DistinctOrAll' has not been implemented yet");
// }
//
// // DotOperator <- '.' (FunctionExpression / ColLabel)
// unique_ptr<SQLStatement> PEGTransformerFactory::TransformDotOperator(PEGTransformer &transformer,
//                                                                      optional_ptr<ParseResult> parse_result) {
// 	auto &list_pr = parse_result->Cast<ListParseResult>();
// 	throw NotImplementedException("Rule 'DotOperator' has not been implemented yet");
// }
//
// // EscapeOperator <- 'ESCAPE'
// unique_ptr<SQLStatement> PEGTransformerFactory::TransformEscapeOperator(PEGTransformer &transformer,
//                                                                         optional_ptr<ParseResult> parse_result) {
// 	auto &list_pr = parse_result->Cast<ListParseResult>();
// 	throw NotImplementedException("Rule 'EscapeOperator' has not been implemented yet");
// }
//
// // ExcludeList <- 'EXCLUDE' (Parens(List(ExcludeName)) / ExcludeName)
// unique_ptr<SQLStatement> PEGTransformerFactory::TransformExcludeList(PEGTransformer &transformer,
//                                                                      optional_ptr<ParseResult> parse_result) {
// 	auto &list_pr = parse_result->Cast<ListParseResult>();
// 	throw NotImplementedException("Rule 'ExcludeList' has not been implemented yet");
// }
//
// // ExcludeName <- DottedIdentifier / ColIdOrString
// unique_ptr<SQLStatement> PEGTransformerFactory::TransformExcludeName(PEGTransformer &transformer,
//                                                                      optional_ptr<ParseResult> parse_result) {
// 	auto &list_pr = parse_result->Cast<ListParseResult>();
// 	throw NotImplementedException("Rule 'ExcludeName' has not been implemented yet");
// }
//
// // ExportClause <- 'EXPORT_STATE'
// unique_ptr<SQLStatement> PEGTransformerFactory::TransformExportClause(PEGTransformer &transformer,
//                                                                       optional_ptr<ParseResult> parse_result) {
// 	auto &list_pr = parse_result->Cast<ListParseResult>();
// 	throw NotImplementedException("Rule 'ExportClause' has not been implemented yet");
// }

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

// // ExtractExpression <- 'EXTRACT' Parens(Expression 'FROM' Expression)
// unique_ptr<SQLStatement> PEGTransformerFactory::TransformExtractExpression(PEGTransformer &transformer,
//                                                                            optional_ptr<ParseResult> parse_result) {
// 	auto &list_pr = parse_result->Cast<ListParseResult>();
// 	throw NotImplementedException("Rule 'ExtractExpression' has not been implemented yet");
// }
//
// // FilterClause <- 'FILTER' Parens('WHERE'? Expression)
// unique_ptr<SQLStatement> PEGTransformerFactory::TransformFilterClause(PEGTransformer &transformer,
//                                                                       optional_ptr<ParseResult> parse_result) {
// 	auto &list_pr = parse_result->Cast<ListParseResult>();
// 	throw NotImplementedException("Rule 'FilterClause' has not been implemented yet");
// }
//
// // FrameBound <- ('UNBOUNDED' 'PRECEDING') / ('UNBOUNDED' 'FOLLOWING') / ('CURRENT' 'ROW') / (Expression 'PRECEDING')
// /
// // (Expression 'FOLLOWING')
// unique_ptr<SQLStatement> PEGTransformerFactory::TransformFrameBound(PEGTransformer &transformer,
//                                                                     optional_ptr<ParseResult> parse_result) {
// 	auto &list_pr = parse_result->Cast<ListParseResult>();
// 	throw NotImplementedException("Rule 'FrameBound' has not been implemented yet");
// }
//
// // FrameClause <- Framing FrameExtent WindowExcludeClause?
// unique_ptr<SQLStatement> PEGTransformerFactory::TransformFrameClause(PEGTransformer &transformer,
//                                                                      optional_ptr<ParseResult> parse_result) {
// 	auto &list_pr = parse_result->Cast<ListParseResult>();
// 	throw NotImplementedException("Rule 'FrameClause' has not been implemented yet");
// }
//
// // FrameExtent <- ('BETWEEN' FrameBound 'AND' FrameBound) / FrameBound
// unique_ptr<SQLStatement> PEGTransformerFactory::TransformFrameExtent(PEGTransformer &transformer,
//                                                                      optional_ptr<ParseResult> parse_result) {
// 	auto &list_pr = parse_result->Cast<ListParseResult>();
// 	throw NotImplementedException("Rule 'FrameExtent' has not been implemented yet");
// }
//
// // Framing <- 'ROWS' / 'RANGE' / 'GROUPS'
// unique_ptr<SQLStatement> PEGTransformerFactory::TransformFraming(PEGTransformer &transformer,
//                                                                  optional_ptr<ParseResult> parse_result) {
// 	auto &list_pr = parse_result->Cast<ListParseResult>();
// 	throw NotImplementedException("Rule 'Framing' has not been implemented yet");
// }
//
// // FunctionExpression <- FunctionIdentifier Parens(DistinctOrAll? List(FunctionArgument)? OrderByClause?
// IgnoreNulls?)
// // WithinGroupClause? FilterClause? ExportClause? OverClause?
// unique_ptr<SQLStatement> PEGTransformerFactory::TransformFunctionExpression(PEGTransformer &transformer,
//                                                                             optional_ptr<ParseResult> parse_result) {
// 	auto &list_pr = parse_result->Cast<ListParseResult>();
// 	throw NotImplementedException("Rule 'FunctionExpression' has not been implemented yet");
// }
//
// // FunctionIdentifier <- CatalogReservedSchemaFunctionName / SchemaReservedFunctionName / FunctionName
// unique_ptr<SQLStatement> PEGTransformerFactory::TransformFunctionIdentifier(PEGTransformer &transformer,
//                                                                             optional_ptr<ParseResult> parse_result) {
// 	auto &list_pr = parse_result->Cast<ListParseResult>();
// 	throw NotImplementedException("Rule 'FunctionIdentifier' has not been implemented yet");
// }
//
// // GroupingExpression <- GroupingOrGroupingId Parens(List(Expression))
// unique_ptr<SQLStatement> PEGTransformerFactory::TransformGroupingExpression(PEGTransformer &transformer,
//                                                                             optional_ptr<ParseResult> parse_result) {
// 	auto &list_pr = parse_result->Cast<ListParseResult>();
// 	throw NotImplementedException("Rule 'GroupingExpression' has not been implemented yet");
// }
//
// // GroupingOrGroupingId <- 'GROUPING' / 'GROUPING_ID'
// unique_ptr<SQLStatement> PEGTransformerFactory::TransformGroupingOrGroupingId(PEGTransformer &transformer,
//                                                                               optional_ptr<ParseResult> parse_result)
//                                                                               {
// 	auto &list_pr = parse_result->Cast<ListParseResult>();
// 	throw NotImplementedException("Rule 'GroupingOrGroupingId' has not been implemented yet");
// }
//
// // IgnoreNulls <- ('IGNORE' 'NULLS') / ('RESPECT' 'NULLS')
// unique_ptr<SQLStatement> PEGTransformerFactory::TransformIgnoreNulls(PEGTransformer &transformer,
//                                                                      optional_ptr<ParseResult> parse_result) {
// 	auto &list_pr = parse_result->Cast<ListParseResult>();
// 	throw NotImplementedException("Rule 'IgnoreNulls' has not been implemented yet");
// }
//
// // InOperator <- 'NOT'? 'IN'
// unique_ptr<SQLStatement> PEGTransformerFactory::TransformInOperator(PEGTransformer &transformer,
//                                                                     optional_ptr<ParseResult> parse_result) {
// 	auto &list_pr = parse_result->Cast<ListParseResult>();
// 	throw NotImplementedException("Rule 'InOperator' has not been implemented yet");
// }
//
// // Indirection <- CastOperator / DotOperator / SliceExpression / NotNull / PostfixOperator
// unique_ptr<SQLStatement> PEGTransformerFactory::TransformIndirection(PEGTransformer &transformer,
//                                                                      optional_ptr<ParseResult> parse_result) {
// 	auto &list_pr = parse_result->Cast<ListParseResult>();
// 	throw NotImplementedException("Rule 'Indirection' has not been implemented yet");
// }
//
// // IntervalLiteral <- 'INTERVAL' IntervalParameter IntervalUnit?
// unique_ptr<SQLStatement> PEGTransformerFactory::TransformIntervalLiteral(PEGTransformer &transformer,
//                                                                          optional_ptr<ParseResult> parse_result) {
// 	auto &list_pr = parse_result->Cast<ListParseResult>();
// 	throw NotImplementedException("Rule 'IntervalLiteral' has not been implemented yet");
// }
//
// // IntervalParameter <- StringLiteral / NumberLiteral / Parens(Expression)
// unique_ptr<SQLStatement> PEGTransformerFactory::TransformIntervalParameter(PEGTransformer &transformer,
//                                                                            optional_ptr<ParseResult> parse_result) {
// 	auto &list_pr = parse_result->Cast<ListParseResult>();
// 	throw NotImplementedException("Rule 'IntervalParameter' has not been implemented yet");
// }
//
// // IntervalUnit <- ColId
// unique_ptr<SQLStatement> PEGTransformerFactory::TransformIntervalUnit(PEGTransformer &transformer,
//                                                                       optional_ptr<ParseResult> parse_result) {
// 	auto &list_pr = parse_result->Cast<ListParseResult>();
// 	throw NotImplementedException("Rule 'IntervalUnit' has not been implemented yet");
// }
//
// // IsOperator <- 'IS' 'NOT'? DistinctFrom?
// unique_ptr<SQLStatement> PEGTransformerFactory::TransformIsOperator(PEGTransformer &transformer,
//                                                                     optional_ptr<ParseResult> parse_result) {
// 	auto &list_pr = parse_result->Cast<ListParseResult>();
// 	throw NotImplementedException("Rule 'IsOperator' has not been implemented yet");
// }
//
// // LambdaExpression <- 'LAMBDA' List(ColIdOrString) ':' Expression
// unique_ptr<SQLStatement> PEGTransformerFactory::TransformLambdaExpression(PEGTransformer &transformer,
//                                                                           optional_ptr<ParseResult> parse_result) {
// 	auto &list_pr = parse_result->Cast<ListParseResult>();
// 	throw NotImplementedException("Rule 'LambdaExpression' has not been implemented yet");
// }
//
// // LambdaOperator <- '->'
// unique_ptr<SQLStatement> PEGTransformerFactory::TransformLambdaOperator(PEGTransformer &transformer,
//                                                                         optional_ptr<ParseResult> parse_result) {
// 	auto &list_pr = parse_result->Cast<ListParseResult>();
// 	throw NotImplementedException("Rule 'LambdaOperator' has not been implemented yet");
// }
//
// // LikeOperator <- 'NOT'? LikeOrSimilarTo
// unique_ptr<SQLStatement> PEGTransformerFactory::TransformLikeOperator(PEGTransformer &transformer,
//                                                                       optional_ptr<ParseResult> parse_result) {
// 	auto &list_pr = parse_result->Cast<ListParseResult>();
// 	throw NotImplementedException("Rule 'LikeOperator' has not been implemented yet");
// }
//
// // LikeOrSimilarTo <- 'LIKE' / 'ILIKE' / 'GLOB' / ('SIMILAR' 'TO')
// unique_ptr<SQLStatement> PEGTransformerFactory::TransformLikeOrSimilarTo(PEGTransformer &transformer,
//                                                                          optional_ptr<ParseResult> parse_result) {
// 	auto &list_pr = parse_result->Cast<ListParseResult>();
// 	throw NotImplementedException("Rule 'LikeOrSimilarTo' has not been implemented yet");
// }
//
// // ListComprehensionExpression <- '[' Expression 'FOR' List(Expression) ListComprehensionFilter? ']'
// unique_ptr<SQLStatement>
// PEGTransformerFactory::TransformListComprehensionExpression(PEGTransformer &transformer,
//                                                             optional_ptr<ParseResult> parse_result) {
// 	auto &list_pr = parse_result->Cast<ListParseResult>();
// 	throw NotImplementedException("Rule 'ListComprehensionExpression' has not been implemented yet");
// }
//
// // ListComprehensionFilter <- 'IF' Expression
// unique_ptr<SQLStatement>
// PEGTransformerFactory::TransformListComprehensionFilter(PEGTransformer &transformer,
//                                                         optional_ptr<ParseResult> parse_result) {
// 	auto &list_pr = parse_result->Cast<ListParseResult>();
// 	throw NotImplementedException("Rule 'ListComprehensionFilter' has not been implemented yet");
// }
//
// // ListExpression <- 'ARRAY'? (BoundedListExpression / SelectStatement)
// unique_ptr<SQLStatement> PEGTransformerFactory::TransformListExpression(PEGTransformer &transformer,
//                                                                         optional_ptr<ParseResult> parse_result) {
// 	auto &list_pr = parse_result->Cast<ListParseResult>();
// 	throw NotImplementedException("Rule 'ListExpression' has not been implemented yet");
// }

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

// // MapExpression <- 'MAP' StructExpression
// unique_ptr<SQLStatement> PEGTransformerFactory::TransformMapExpression(PEGTransformer &transformer,
//                                                                        optional_ptr<ParseResult> parse_result) {
// 	auto &list_pr = parse_result->Cast<ListParseResult>();
// 	throw NotImplementedException("Rule 'MapExpression' has not been implemented yet");
// }
//
// // NotNull <- 'NOT' 'NULL'
// unique_ptr<SQLStatement> PEGTransformerFactory::TransformNotNull(PEGTransformer &transformer,
//                                                                  optional_ptr<ParseResult> parse_result) {
// 	auto &list_pr = parse_result->Cast<ListParseResult>();
// 	throw NotImplementedException("Rule 'NotNull' has not been implemented yet");
// }
//
// // NullIfExpression <- 'NULLIF' Parens(Expression ',' Expression)
// unique_ptr<SQLStatement> PEGTransformerFactory::TransformNullIfExpression(PEGTransformer &transformer,
//                                                                           optional_ptr<ParseResult> parse_result) {
// 	auto &list_pr = parse_result->Cast<ListParseResult>();
// 	throw NotImplementedException("Rule 'NullIfExpression' has not been implemented yet");
// }
//
// // NumberedParameter <- '$' NumberLiteral
// unique_ptr<SQLStatement> PEGTransformerFactory::TransformNumberedParameter(PEGTransformer &transformer,
//                                                                            optional_ptr<ParseResult> parse_result) {
// 	auto &list_pr = parse_result->Cast<ListParseResult>();
// 	throw NotImplementedException("Rule 'NumberedParameter' has not been implemented yet");
// }
//
// // Operator <- AnyAllOperator /
// // ConjunctionOperator /
// // LikeOperator /
// // InOperator /
// // IsOperator /
// // BetweenOperator /
// // CollateOperator /
// // LambdaOperator /
// // EscapeOperator /
// // AtTimeZoneOperator /
// // OperatorLiteral
// unique_ptr<SQLStatement> PEGTransformerFactory::TransformOperator(PEGTransformer &transformer,
//                                                                   optional_ptr<ParseResult> parse_result) {
// 	auto &list_pr = parse_result->Cast<ListParseResult>();
// 	throw NotImplementedException("Rule 'Operator' has not been implemented yet");
// }
//
// // OperatorLiteral <- <[\+\-\*\/\%\^\<\>\=\~\!\@\&\|\`]+>
// unique_ptr<SQLStatement> PEGTransformerFactory::TransformOperatorLiteral(PEGTransformer &transformer,
//                                                                          optional_ptr<ParseResult> parse_result) {
// 	auto &list_pr = parse_result->Cast<ListParseResult>();
// 	throw NotImplementedException("Rule 'OperatorLiteral' has not been implemented yet");
// }
//
// // OverClause <- 'OVER' WindowFrame
// unique_ptr<SQLStatement> PEGTransformerFactory::TransformOverClause(PEGTransformer &transformer,
//                                                                     optional_ptr<ParseResult> parse_result) {
// 	auto &list_pr = parse_result->Cast<ListParseResult>();
// 	throw NotImplementedException("Rule 'OverClause' has not been implemented yet");
// }
//
// // Parameter <- '?' / NumberedParameter / ColLabelParameter
// unique_ptr<SQLStatement> PEGTransformerFactory::TransformParameter(PEGTransformer &transformer,
//                                                                    optional_ptr<ParseResult> parse_result) {
// 	auto &list_pr = parse_result->Cast<ListParseResult>();
// 	throw NotImplementedException("Rule 'Parameter' has not been implemented yet");
// }
//
// // ParenthesisExpression <- Parens(List(Expression))
// unique_ptr<SQLStatement> PEGTransformerFactory::TransformParenthesisExpression(PEGTransformer &transformer,
//                                                                                optional_ptr<ParseResult>
//                                                                                parse_result) {
// 	auto &list_pr = parse_result->Cast<ListParseResult>();
// 	throw NotImplementedException("Rule 'ParenthesisExpression' has not been implemented yet");
// }
//
// // PositionExpression <- 'POSITION' Parens(Expression)
// unique_ptr<SQLStatement> PEGTransformerFactory::TransformPositionExpression(PEGTransformer &transformer,
//                                                                             optional_ptr<ParseResult> parse_result) {
// 	auto &list_pr = parse_result->Cast<ListParseResult>();
// 	throw NotImplementedException("Rule 'PositionExpression' has not been implemented yet");
// }
//
// // PositionalExpression <- '#' NumberLiteral
// unique_ptr<SQLStatement> PEGTransformerFactory::TransformPositionalExpression(PEGTransformer &transformer,
//                                                                               optional_ptr<ParseResult> parse_result)
//                                                                               {
// 	auto &list_pr = parse_result->Cast<ListParseResult>();
// 	throw NotImplementedException("Rule 'PositionalExpression' has not been implemented yet");
// }
//
// // PostfixOperator <- '!'
// unique_ptr<SQLStatement> PEGTransformerFactory::TransformPostfixOperator(PEGTransformer &transformer,
//                                                                          optional_ptr<ParseResult> parse_result) {
// 	auto &list_pr = parse_result->Cast<ListParseResult>();
// 	throw NotImplementedException("Rule 'PostfixOperator' has not been implemented yet");
// }
//
// // PrefixExpression <- PrefixOperator Expression
// unique_ptr<SQLStatement> PEGTransformerFactory::TransformPrefixExpression(PEGTransformer &transformer,
//                                                                           optional_ptr<ParseResult> parse_result) {
// 	auto &list_pr = parse_result->Cast<ListParseResult>();
// 	throw NotImplementedException("Rule 'PrefixExpression' has not been implemented yet");
// }
//
// // PrefixOperator <- 'NOT' / '-' / '+' / '~'
// unique_ptr<SQLStatement> PEGTransformerFactory::TransformPrefixOperator(PEGTransformer &transformer,
//                                                                         optional_ptr<ParseResult> parse_result) {
// 	auto &list_pr = parse_result->Cast<ListParseResult>();
// 	throw NotImplementedException("Rule 'PrefixOperator' has not been implemented yet");
// }
//
// // RecursiveExpression <- (Operator Expression)
// unique_ptr<SQLStatement> PEGTransformerFactory::TransformRecursiveExpression(PEGTransformer &transformer,
//                                                                              optional_ptr<ParseResult> parse_result)
//                                                                              {
// 	auto &list_pr = parse_result->Cast<ListParseResult>();
// 	throw NotImplementedException("Rule 'RecursiveExpression' has not been implemented yet");
// }
//
// // RenameEntry <- ColumnReference 'AS' Identifier
// unique_ptr<SQLStatement> PEGTransformerFactory::TransformRenameEntry(PEGTransformer &transformer,
//                                                                      optional_ptr<ParseResult> parse_result) {
// 	auto &list_pr = parse_result->Cast<ListParseResult>();
// 	throw NotImplementedException("Rule 'RenameEntry' has not been implemented yet");
// }
//
// // RenameList <- 'RENAME' (Parens(List(RenameEntry)) / RenameEntry)
// unique_ptr<SQLStatement> PEGTransformerFactory::TransformRenameList(PEGTransformer &transformer,
//                                                                     optional_ptr<ParseResult> parse_result) {
// 	auto &list_pr = parse_result->Cast<ListParseResult>();
// 	throw NotImplementedException("Rule 'RenameList' has not been implemented yet");
// }
//
// // ReplaceEntry <- Expression 'AS' ColumnReference
// unique_ptr<SQLStatement> PEGTransformerFactory::TransformReplaceEntry(PEGTransformer &transformer,
//                                                                       optional_ptr<ParseResult> parse_result) {
// 	auto &list_pr = parse_result->Cast<ListParseResult>();
// 	throw NotImplementedException("Rule 'ReplaceEntry' has not been implemented yet");
// }
//
// // ReplaceList <- 'REPLACE' (Parens(List(ReplaceEntry)) / ReplaceEntry)
// unique_ptr<SQLStatement> PEGTransformerFactory::TransformReplaceList(PEGTransformer &transformer,
//                                                                      optional_ptr<ParseResult> parse_result) {
// 	auto &list_pr = parse_result->Cast<ListParseResult>();
// 	throw NotImplementedException("Rule 'ReplaceList' has not been implemented yet");
// }
//
// // RowExpression <- 'ROW' Parens(List(Expression))
// unique_ptr<SQLStatement> PEGTransformerFactory::TransformRowExpression(PEGTransformer &transformer,
//                                                                        optional_ptr<ParseResult> parse_result) {
// 	auto &list_pr = parse_result->Cast<ListParseResult>();
// 	throw NotImplementedException("Rule 'RowExpression' has not been implemented yet");
// }
//
// // SchemaReservedFunctionName <- SchemaQualification ReservedFunctionName
// unique_ptr<SQLStatement>
// PEGTransformerFactory::TransformSchemaReservedFunctionName(PEGTransformer &transformer,
//                                                            optional_ptr<ParseResult> parse_result) {
// 	auto &list_pr = parse_result->Cast<ListParseResult>();
// 	throw NotImplementedException("Rule 'SchemaReservedFunctionName' has not been implemented yet");
// }
//
// // SchemaReservedTableColumnName <- SchemaQualification ReservedTableQualification ReservedColumnName
// unique_ptr<SQLStatement>
// PEGTransformerFactory::TransformSchemaReservedTableColumnName(PEGTransformer &transformer,
//                                                               optional_ptr<ParseResult> parse_result) {
// 	auto &list_pr = parse_result->Cast<ListParseResult>();
// 	throw NotImplementedException("Rule 'SchemaReservedTableColumnName' has not been implemented yet");
// }
//
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

// // SliceBound <- Expression? (':' (Expression / '-')?)? (':' Expression?)?
// unique_ptr<SQLStatement> PEGTransformerFactory::TransformSliceBound(PEGTransformer &transformer,
//                                                                     optional_ptr<ParseResult> parse_result) {
// 	auto &list_pr = parse_result->Cast<ListParseResult>();
// 	throw NotImplementedException("Rule 'SliceBound' has not been implemented yet");
// }
//
// // SliceExpression <- '[' SliceBound ']'
// unique_ptr<SQLStatement> PEGTransformerFactory::TransformSliceExpression(PEGTransformer &transformer,
//                                                                          optional_ptr<ParseResult> parse_result) {
// 	auto &list_pr = parse_result->Cast<ListParseResult>();
// 	throw NotImplementedException("Rule 'SliceExpression' has not been implemented yet");
// }
//
// // SpecialFunctionExpression <- CoalesceExpression / UnpackExpression / ColumnsExpression / ExtractExpression /
// // LambdaExpression / NullIfExpression / PositionExpression / RowExpression / SubstringExpression / TrimExpression
// unique_ptr<SQLStatement>
// PEGTransformerFactory::TransformSpecialFunctionExpression(PEGTransformer &transformer,
//                                                           optional_ptr<ParseResult> parse_result) {
// 	auto &list_pr = parse_result->Cast<ListParseResult>();
// 	throw NotImplementedException("Rule 'SpecialFunctionExpression' has not been implemented yet");
// }
//
// // StarExpression <- (ColId '.')* '*' ExcludeList? ReplaceList? RenameList?
// unique_ptr<SQLStatement> PEGTransformerFactory::TransformStarExpression(PEGTransformer &transformer,
//                                                                         optional_ptr<ParseResult> parse_result) {
// 	auto &list_pr = parse_result->Cast<ListParseResult>();
// 	throw NotImplementedException("Rule 'StarExpression' has not been implemented yet");
// }
//
// // StructExpression <- '{' List(StructField)? '}'
// unique_ptr<SQLStatement> PEGTransformerFactory::TransformStructExpression(PEGTransformer &transformer,
//                                                                           optional_ptr<ParseResult> parse_result) {
// 	auto &list_pr = parse_result->Cast<ListParseResult>();
// 	throw NotImplementedException("Rule 'StructExpression' has not been implemented yet");
// }
//
// // StructField <- Expression ':' Expression
// unique_ptr<SQLStatement> PEGTransformerFactory::TransformStructField(PEGTransformer &transformer,
//                                                                      optional_ptr<ParseResult> parse_result) {
// 	auto &list_pr = parse_result->Cast<ListParseResult>();
// 	throw NotImplementedException("Rule 'StructField' has not been implemented yet");
// }
//
// // SubqueryExpression <- 'NOT'? 'EXISTS'? SubqueryReference
// unique_ptr<SQLStatement> PEGTransformerFactory::TransformSubqueryExpression(PEGTransformer &transformer,
//                                                                             optional_ptr<ParseResult> parse_result) {
// 	auto &list_pr = parse_result->Cast<ListParseResult>();
// 	throw NotImplementedException("Rule 'SubqueryExpression' has not been implemented yet");
// }
//
// // SubstringExpression <- 'SUBSTRING' Parens(SubstringParameters / List(Expression))
// unique_ptr<SQLStatement> PEGTransformerFactory::TransformSubstringExpression(PEGTransformer &transformer,
//                                                                              optional_ptr<ParseResult> parse_result)
//                                                                              {
// 	auto &list_pr = parse_result->Cast<ListParseResult>();
// 	throw NotImplementedException("Rule 'SubstringExpression' has not been implemented yet");
// }
//
// // SubstringParameters <- Expression 'FROM' NumberLiteral 'FOR' NumberLiteral
// unique_ptr<SQLStatement> PEGTransformerFactory::TransformSubstringParameters(PEGTransformer &transformer,
//                                                                              optional_ptr<ParseResult> parse_result)
//                                                                              {
// 	auto &list_pr = parse_result->Cast<ListParseResult>();
// 	throw NotImplementedException("Rule 'SubstringParameters' has not been implemented yet");
// }
//
// // TableReservedColumnName <- TableQualification ReservedColumnName
// unique_ptr<SQLStatement>
// PEGTransformerFactory::TransformTableReservedColumnName(PEGTransformer &transformer,
//                                                         optional_ptr<ParseResult> parse_result) {
// 	auto &list_pr = parse_result->Cast<ListParseResult>();
// 	throw NotImplementedException("Rule 'TableReservedColumnName' has not been implemented yet");
// }
//
// // TrimDirection <- 'BOTH' / 'LEADING' / 'TRAILING'
// unique_ptr<SQLStatement> PEGTransformerFactory::TransformTrimDirection(PEGTransformer &transformer,
//                                                                        optional_ptr<ParseResult> parse_result) {
// 	auto &list_pr = parse_result->Cast<ListParseResult>();
// 	throw NotImplementedException("Rule 'TrimDirection' has not been implemented yet");
// }
//
// // TrimExpression <- 'TRIM' Parens(TrimDirection? TrimSource? List(Expression))
// unique_ptr<SQLStatement> PEGTransformerFactory::TransformTrimExpression(PEGTransformer &transformer,
//                                                                         optional_ptr<ParseResult> parse_result) {
// 	auto &list_pr = parse_result->Cast<ListParseResult>();
// 	throw NotImplementedException("Rule 'TrimExpression' has not been implemented yet");
// }
//
// // TrimSource <- Expression? 'FROM'
// unique_ptr<SQLStatement> PEGTransformerFactory::TransformTrimSource(PEGTransformer &transformer,
//                                                                     optional_ptr<ParseResult> parse_result) {
// 	auto &list_pr = parse_result->Cast<ListParseResult>();
// 	throw NotImplementedException("Rule 'TrimSource' has not been implemented yet");
// }
//
// // TypeLiteral <- ColId StringLiteral
// unique_ptr<SQLStatement> PEGTransformerFactory::TransformTypeLiteral(PEGTransformer &transformer,
//                                                                      optional_ptr<ParseResult> parse_result) {
// 	auto &list_pr = parse_result->Cast<ListParseResult>();
// 	throw NotImplementedException("Rule 'TypeLiteral' has not been implemented yet");
// }
//
// // UnpackExpression <- 'UNPACK' Parens(Expression)
// unique_ptr<SQLStatement> PEGTransformerFactory::TransformUnpackExpression(PEGTransformer &transformer,
//                                                                           optional_ptr<ParseResult> parse_result) {
// 	auto &list_pr = parse_result->Cast<ListParseResult>();
// 	throw NotImplementedException("Rule 'UnpackExpression' has not been implemented yet");
// }
//
// // WindowExcludeClause <- 'EXCLUDE' WindowExcludeElement
// unique_ptr<SQLStatement> PEGTransformerFactory::TransformWindowExcludeClause(PEGTransformer &transformer,
//                                                                              optional_ptr<ParseResult> parse_result)
//                                                                              {
// 	auto &list_pr = parse_result->Cast<ListParseResult>();
// 	throw NotImplementedException("Rule 'WindowExcludeClause' has not been implemented yet");
// }
//
// // WindowExcludeElement <- ('CURRENT' 'ROW') / 'GROUP' / 'TIES' / ('NO' 'OTHERS')
// unique_ptr<SQLStatement> PEGTransformerFactory::TransformWindowExcludeElement(PEGTransformer &transformer,
//                                                                               optional_ptr<ParseResult> parse_result)
//                                                                               {
// 	auto &list_pr = parse_result->Cast<ListParseResult>();
// 	throw NotImplementedException("Rule 'WindowExcludeElement' has not been implemented yet");
// }
//
// // WindowFrame <- WindowFrameDefinition / Identifier / Parens(Identifier)
// unique_ptr<SQLStatement> PEGTransformerFactory::TransformWindowFrame(PEGTransformer &transformer,
//                                                                      optional_ptr<ParseResult> parse_result) {
// 	auto &list_pr = parse_result->Cast<ListParseResult>();
// 	throw NotImplementedException("Rule 'WindowFrame' has not been implemented yet");
// }
//
// // WindowFrameContents <- WindowPartition? OrderByClause? FrameClause?
// unique_ptr<SQLStatement> PEGTransformerFactory::TransformWindowFrameContents(PEGTransformer &transformer,
//                                                                              optional_ptr<ParseResult> parse_result)
//                                                                              {
// 	auto &list_pr = parse_result->Cast<ListParseResult>();
// 	throw NotImplementedException("Rule 'WindowFrameContents' has not been implemented yet");
// }
//
// // WindowFrameDefinition <- Parens(BaseWindowName? WindowFrameContents) / Parens(WindowFrameContents)
// unique_ptr<SQLStatement> PEGTransformerFactory::TransformWindowFrameDefinition(PEGTransformer &transformer,
//                                                                                optional_ptr<ParseResult>
//                                                                                parse_result) {
// 	auto &list_pr = parse_result->Cast<ListParseResult>();
// 	throw NotImplementedException("Rule 'WindowFrameDefinition' has not been implemented yet");
// }
//
// // WindowPartition <- 'PARTITION' 'BY' List(Expression)
// unique_ptr<SQLStatement> PEGTransformerFactory::TransformWindowPartition(PEGTransformer &transformer,
//                                                                          optional_ptr<ParseResult> parse_result) {
// 	auto &list_pr = parse_result->Cast<ListParseResult>();
// 	throw NotImplementedException("Rule 'WindowPartition' has not been implemented yet");
// }
//
// // WithinGroupClause <- 'WITHIN' 'GROUP' Parens(OrderByClause)
// unique_ptr<SQLStatement> PEGTransformerFactory::TransformWithinGroupClause(PEGTransformer &transformer,
//                                                                            optional_ptr<ParseResult> parse_result) {
// 	auto &list_pr = parse_result->Cast<ListParseResult>();
// 	throw NotImplementedException("Rule 'WithinGroupClause' has not been implemented yet");
// }
} // namespace duckdb
