#include "duckdb/parser/peg/ast/generic_copy_option.hpp"
#include "duckdb/parser/expression/cast_expression.hpp"
#include "duckdb/parser/expression/columnref_expression.hpp"
#include "duckdb/parser/expression/constant_expression.hpp"
#include "duckdb/parser/expression/function_expression.hpp"
#include "duckdb/parser/expression/operator_expression.hpp"
#include "duckdb/parser/peg/transformer/peg_transformer.hpp"

namespace duckdb {

vector<GenericCopyOption>
PEGTransformerFactory::TransformGenericCopyOptionList(PEGTransformer &transformer,
                                                      const vector<GenericCopyOption> &generic_copy_option) {
	return generic_copy_option;
}

static void SetGenericCopyOptionExpression(GenericCopyOption &copy_option, unique_ptr<ParsedExpression> expression) {
	if (expression->GetExpressionType() == ExpressionType::VALUE_CONSTANT) {
		copy_option.children.push_back(Value(expression->Cast<ConstantExpression>().GetValue()));
	} else if (expression->GetExpressionType() == ExpressionType::COLUMN_REF) {
		copy_option.children.push_back(Value(expression->Cast<ColumnRefExpression>().GetColumnName()));
	} else if (expression->GetExpressionType() == ExpressionType::PLACEHOLDER) {
		auto &op_expr = expression->Cast<OperatorExpression>();
		for (auto &child : op_expr.GetChildren()) {
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
		if (cast_expr.Child().GetExpressionClass() == ExpressionClass::CONSTANT) {
			auto &const_expr = cast_expr.Child().Cast<ConstantExpression>();
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

static unique_ptr<ParsedExpression> CreateRowFunction(vector<unique_ptr<ParsedExpression>> &&children) {
	return make_uniq<FunctionExpression>(INVALID_CATALOG, DEFAULT_SCHEMA, "row", std::move(children));
}

static unique_ptr<ParsedExpression> CreateOrderByRowFunction(const vector<OrderByNode> &orders) {
	vector<unique_ptr<ParsedExpression>> children;
	children.reserve(orders.size());
	for (auto &order : orders) {
		children.push_back(make_uniq<ConstantExpression>(Value(order.ToString())));
	}
	return CreateRowFunction(std::move(children));
}

static unique_ptr<ParsedExpression> CreateExpressionRowFunction(vector<OrderByNode> &orders) {
	vector<unique_ptr<ParsedExpression>> children;
	children.reserve(orders.size());
	for (auto &order : orders) {
		children.push_back(std::move(order.expression));
	}
	return CreateRowFunction(std::move(children));
}

GenericCopyOption
PEGTransformerFactory::TransformGenericCopyOption(PEGTransformer &transformer, const Identifier &copy_option_name,
                                                  optional<GenericCopyOptionValue> generic_copy_option_value) {
	GenericCopyOption copy_option;
	copy_option.name = Identifier(StringUtil::Lower(copy_option_name.GetIdentifierName()));
	if (!generic_copy_option_value || !generic_copy_option_value->has_value) {
		return copy_option;
	}

	if (generic_copy_option_value->is_order_list) {
		auto &orders = generic_copy_option_value->order_list;
		bool has_order_modifier = false;
		for (auto &order : orders) {
			if (order.type != OrderType::ORDER_DEFAULT || order.null_order != OrderByNullType::ORDER_DEFAULT) {
				has_order_modifier = true;
				break;
			}
		}

		if (copy_option.name == "ORDER_BY") {
			copy_option.expression = CreateOrderByRowFunction(orders);
		} else if (has_order_modifier) {
			throw ParserException("ORDER BY modifiers are only supported in the ORDER_BY option");
		} else if (orders.size() == 1) {
			SetGenericCopyOptionExpression(copy_option, std::move(orders[0].expression));
		} else {
			copy_option.expression = CreateExpressionRowFunction(generic_copy_option_value->order_list);
		}
	} else {
		SetGenericCopyOptionExpression(copy_option, std::move(generic_copy_option_value->expression));
	}
	return copy_option;
}

GenericCopyOptionValue PEGTransformerFactory::TransformGenericCopyOptionOrderList(
    PEGTransformer &transformer, vector<OrderByNode> generic_copy_option_parenthesized_expression_list) {
	GenericCopyOptionValue result;
	result.has_value = true;
	result.is_order_list = true;
	result.order_list = std::move(generic_copy_option_parenthesized_expression_list);
	return result;
}

GenericCopyOptionValue
PEGTransformerFactory::TransformGenericCopyOptionExpression(PEGTransformer &transformer,
                                                            unique_ptr<ParsedExpression> expression) {
	GenericCopyOptionValue result;
	result.has_value = true;
	result.expression = std::move(expression);
	return result;
}

vector<OrderByNode> PEGTransformerFactory::TransformGenericCopyOptionParenthesizedExpressionList(
    PEGTransformer &transformer, vector<OrderByNode> order_by_expression_list) {
	return order_by_expression_list;
}

} // namespace duckdb
