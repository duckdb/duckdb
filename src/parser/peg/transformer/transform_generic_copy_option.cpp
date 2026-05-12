#include "duckdb/parser/peg/ast/generic_copy_option.hpp"
#include "duckdb/parser/expression/cast_expression.hpp"
#include "duckdb/parser/expression/columnref_expression.hpp"
#include "duckdb/parser/expression/function_expression.hpp"
#include "duckdb/parser/expression/operator_expression.hpp"
#include "duckdb/parser/peg/transformer/peg_transformer.hpp"

namespace duckdb {

vector<GenericCopyOption> PEGTransformerFactory::TransformGenericCopyOptionList(PEGTransformer &transformer,
                                                                                ParseResult &parse_result) {
	vector<GenericCopyOption> result;
	auto &list_pr = parse_result.Cast<ListParseResult>();
	auto &extract_parens = ExtractResultFromParens(list_pr.Child<ListParseResult>(0));
	auto option_list = ExtractParseResultsFromList(extract_parens);
	for (auto &option : option_list) {
		result.push_back(transformer.Transform<GenericCopyOption>(option));
	}
	return result;
}

static void SetGenericCopyOptionExpression(GenericCopyOption &copy_option, unique_ptr<ParsedExpression> expression) {
	if (expression->GetExpressionType() == ExpressionType::VALUE_CONSTANT) {
		copy_option.children.push_back(Value(expression->Cast<ConstantExpression>().GetValue()));
	} else if (expression->GetExpressionType() == ExpressionType::COLUMN_REF) {
		copy_option.children.push_back(Value(expression->Cast<ColumnRefExpression>().GetColumnName()));
	} else if (expression->GetExpressionType() == ExpressionType::PLACEHOLDER) {
		auto &op_expr = expression->Cast<OperatorExpression>();
		for (auto &child : op_expr.children) {
			if (child->GetExpressionClass() == ExpressionClass::CONSTANT) {
				copy_option.children.push_back(Value(child->Cast<ConstantExpression>().GetValue()));
			} else if (child->GetExpressionClass() == ExpressionClass::COLUMN_REF) {
				copy_option.children.push_back(Value(child->Cast<ColumnRefExpression>().GetColumnName()));
			} else {
				throw InternalException("Unexpected expression type %s encountered for GenericCopyOption",
				                        ExpressionClassToString(child->GetExpressionClass()));
			}
		}
	} else if (expression->GetExpressionType() == ExpressionType::FUNCTION) {
		copy_option.expression = std::move(expression);
	} else if (expression->GetExpressionType() == ExpressionType::STAR) {
		copy_option.children.push_back(Value("*"));
	} else if (expression->GetExpressionType() == ExpressionType::OPERATOR_CAST) {
		auto &cast_expr = expression->Cast<CastExpression>();
		if (cast_expr.child->GetExpressionClass() == ExpressionClass::CONSTANT) {
			auto &const_expr = cast_expr.child->Cast<ConstantExpression>();
			if (const_expr.GetValue().GetValue<string>() == "t") {
				copy_option.children.push_back(Value(true));
			} else if (const_expr.GetValue().GetValue<string>() == "f") {
				copy_option.children.push_back(Value(false));
			} else {
				copy_option.expression = std::move(expression);
			}
		} else {
			copy_option.expression = std::move(expression);
		}
	} else {
		throw NotImplementedException("Unrecognized expression type %s",
		                              ExpressionTypeToString(expression->GetExpressionType()));
	}
}

GenericCopyOption PEGTransformerFactory::TransformGenericCopyOption(PEGTransformer &transformer,
                                                                    ParseResult &parse_result) {
	GenericCopyOption copy_option;

	auto &list_pr = parse_result.Cast<ListParseResult>();
	copy_option.name = StringUtil::Lower(list_pr.Child<IdentifierParseResult>(0).identifier);
	auto &option_value = list_pr.Child<OptionalParseResult>(1);
	if (!option_value.HasResult()) {
		return copy_option;
	}

	auto &value_choice = option_value.GetResult().Cast<ListParseResult>().Child<ChoiceParseResult>(0).GetResult();
	if (value_choice.name == "GenericCopyOptionParenthesizedExpressionList") {
		auto orders = transformer.Transform<vector<OrderByNode>>(value_choice);
		bool has_order_modifier = false;
		for (auto &order : orders) {
			if (order.type != OrderType::ORDER_DEFAULT || order.null_order != OrderByNullType::ORDER_DEFAULT) {
				has_order_modifier = true;
				break;
			}
		}

		if (StringUtil::CIEquals(copy_option.name, "ORDER_BY")) {
			for (auto &order : orders) {
				copy_option.children.emplace_back(order.ToString());
			}
		} else if (has_order_modifier) {
			throw ParserException("ORDER BY modifiers are only supported in the ORDER_BY option");
		} else if (orders.size() == 1) {
			SetGenericCopyOptionExpression(copy_option, std::move(orders[0].expression));
		} else {
			vector<unique_ptr<ParsedExpression>> children;
			for (auto &order : orders) {
				children.push_back(std::move(order.expression));
			}
			copy_option.expression =
			    make_uniq<FunctionExpression>(INVALID_CATALOG, DEFAULT_SCHEMA, "row", std::move(children));
		}
	} else {
		auto expression = transformer.Transform<unique_ptr<ParsedExpression>>(value_choice);
		SetGenericCopyOptionExpression(copy_option, std::move(expression));
	}
	return copy_option;
}

vector<OrderByNode>
PEGTransformerFactory::TransformGenericCopyOptionParenthesizedExpressionList(PEGTransformer &transformer,
                                                                             ParseResult &parse_result) {
	auto &list_pr = parse_result.Cast<ListParseResult>();
	auto &extract_parens = ExtractResultFromParens(list_pr.Child<ListParseResult>(0));
	return transformer.Transform<vector<OrderByNode>>(extract_parens);
}

} // namespace duckdb
