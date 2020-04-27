#include "duckdb/parser/expression/case_expression.hpp"
#include "duckdb/parser/expression/cast_expression.hpp"
#include "duckdb/parser/expression/comparison_expression.hpp"
#include "duckdb/parser/expression/conjunction_expression.hpp"
#include "duckdb/parser/expression/constant_expression.hpp"
#include "duckdb/parser/expression/function_expression.hpp"
#include "duckdb/parser/expression/operator_expression.hpp"
#include "duckdb/parser/transformer.hpp"

using namespace duckdb;
using namespace std;

ExpressionType Transformer::OperatorToExpressionType(string &op) {
	if (op == "=" || op == "==") {
		return ExpressionType::COMPARE_EQUAL;
	} else if (op == "!=" || op == "<>") {
		return ExpressionType::COMPARE_NOTEQUAL;
	} else if (op == "<") {
		return ExpressionType::COMPARE_LESSTHAN;
	} else if (op == ">") {
		return ExpressionType::COMPARE_GREATERTHAN;
	} else if (op == "<=") {
		return ExpressionType::COMPARE_LESSTHANOREQUALTO;
	} else if (op == ">=") {
		return ExpressionType::COMPARE_GREATERTHANOREQUALTO;
	}
	return ExpressionType::INVALID;
}

unique_ptr<ParsedExpression> Transformer::TransformUnaryOperator(string op, unique_ptr<ParsedExpression> child) {
	const auto schema = DEFAULT_SCHEMA;

	vector<unique_ptr<ParsedExpression>> children;
	children.push_back(move(child));

	// built-in operator function
	auto result = make_unique<FunctionExpression>(schema, op, children);
	result->is_operator = true;
	return move(result);
}

unique_ptr<ParsedExpression> Transformer::TransformBinaryOperator(string op, unique_ptr<ParsedExpression> left,
                                                                  unique_ptr<ParsedExpression> right) {
	const auto schema = DEFAULT_SCHEMA;

	vector<unique_ptr<ParsedExpression>> children;
	children.push_back(move(left));
	children.push_back(move(right));

	if (op == "~" || op == "!~") {
		// rewrite 'asdf' SIMILAR TO '.*sd.*' into regexp_full_match('asdf', '.*sd.*')
		bool invert_similar = op == "!~";

		auto result = make_unique<FunctionExpression>(schema, "regexp_full_match", children);
		if (invert_similar) {
			return make_unique<OperatorExpression>(ExpressionType::OPERATOR_NOT, move(result));
		} else {
			return move(result);
		}
	} else {
		auto target_type = OperatorToExpressionType(op);
		if (target_type != ExpressionType::INVALID) {
			// built-in comparison operator
			return make_unique<ComparisonExpression>(target_type, move(children[0]), move(children[1]));
		} else {
			// built-in operator function
			auto result = make_unique<FunctionExpression>(schema, op, children);
			result->is_operator = true;
			return move(result);
		}
	}
}

unique_ptr<ParsedExpression> Transformer::TransformAExpr(PGAExpr *root) {
	if (!root) {
		return nullptr;
	}
	auto name = string((reinterpret_cast<PGValue *>(root->name->head->data.ptr_value))->val.str);

	switch (root->kind) {
	case PG_AEXPR_DISTINCT:
		break;
	case PG_AEXPR_IN: {
		auto left_expr = TransformExpression(root->lexpr);
		ExpressionType operator_type;
		// this looks very odd, but seems to be the way to find out its NOT IN
		if (name == "<>") {
			// NOT IN
			operator_type = ExpressionType::COMPARE_NOT_IN;
		} else {
			// IN
			operator_type = ExpressionType::COMPARE_IN;
		}
		auto result = make_unique<OperatorExpression>(operator_type, move(left_expr));
		TransformExpressionList((PGList *)root->rexpr, result->children);
		return move(result);
	} break;
	// rewrite NULLIF(a, b) into CASE WHEN a=b THEN NULL ELSE a END
	case PG_AEXPR_NULLIF: {
		auto case_expr = make_unique<CaseExpression>();
		auto value = TransformExpression(root->lexpr);
		// the check (A = B)
		case_expr->check = make_unique<ComparisonExpression>(ExpressionType::COMPARE_EQUAL, value->Copy(),
		                                                     TransformExpression(root->rexpr));
		// if A = B, then constant NULL
		case_expr->result_if_true = make_unique<ConstantExpression>(SQLType::SQLNULL, Value());
		// else A
		case_expr->result_if_false = move(value);
		return move(case_expr);
	} break;
	// rewrite (NOT) X BETWEEN A AND B into (NOT) AND(GREATERTHANOREQUALTO(X,
	// A), LESSTHANOREQUALTO(X, B))
	case PG_AEXPR_BETWEEN:
	case PG_AEXPR_NOT_BETWEEN: {
		auto between_args = reinterpret_cast<PGList *>(root->rexpr);

		if (between_args->length != 2 || !between_args->head->data.ptr_value || !between_args->tail->data.ptr_value) {
			throw Exception("(NOT) BETWEEN needs two args");
		}

		auto between_left = TransformExpression(reinterpret_cast<PGNode *>(between_args->head->data.ptr_value));
		auto between_right = TransformExpression(reinterpret_cast<PGNode *>(between_args->tail->data.ptr_value));

		auto compare_left = make_unique<ComparisonExpression>(ExpressionType::COMPARE_GREATERTHANOREQUALTO,
		                                                      TransformExpression(root->lexpr), move(between_left));
		auto compare_right = make_unique<ComparisonExpression>(ExpressionType::COMPARE_LESSTHANOREQUALTO,
		                                                       TransformExpression(root->lexpr), move(between_right));
		auto compare_between = make_unique<ConjunctionExpression>(ExpressionType::CONJUNCTION_AND, move(compare_left),
		                                                          move(compare_right));
		if (root->kind == PG_AEXPR_BETWEEN) {
			return move(compare_between);
		} else {
			return make_unique<OperatorExpression>(ExpressionType::OPERATOR_NOT, move(compare_between));
		}
	} break;
	// rewrite SIMILAR TO into regexp_full_match('asdf', '.*sd.*')
	case PG_AEXPR_SIMILAR: {
		auto left_expr = TransformExpression(root->lexpr);
		auto right_expr = TransformExpression(root->rexpr);

		vector<unique_ptr<ParsedExpression>> children;
		children.push_back(move(left_expr));

		auto &similar_func = reinterpret_cast<FunctionExpression &>(*right_expr);
		assert(similar_func.function_name == "similar_escape");
		assert(similar_func.children.size() == 2);
		if (similar_func.children[1]->type != ExpressionType::VALUE_CONSTANT) {
			throw NotImplementedException("Custom escape in SIMILAR TO");
		}
		auto &constant = (ConstantExpression &)*similar_func.children[1];
		if (!constant.value.is_null) {
			throw NotImplementedException("Custom escape in SIMILAR TO");
		}
		// take the child of the similar_func
		children.push_back(move(similar_func.children[0]));

		// this looks very odd, but seems to be the way to find out its NOT IN
		bool invert_similar = false;
		if (name == "!~") {
			// NOT SIMILAR TO
			invert_similar = true;
		}
		const auto schema = DEFAULT_SCHEMA;
		const auto regex_function = "regexp_full_match";
		auto result = make_unique<FunctionExpression>(schema, regex_function, children);

		if (invert_similar) {
			return make_unique<OperatorExpression>(ExpressionType::OPERATOR_NOT, move(result));
		} else {
			return move(result);
		}
	} break;
	default:
		break;
	}
	auto left_expr = TransformExpression(root->lexpr);
	auto right_expr = TransformExpression(root->rexpr);

	if (!left_expr) {
		// prefix operator
		return TransformUnaryOperator(name, move(right_expr));
	} else if (!right_expr) {
		throw NotImplementedException("Postfix operators not implemented!");
	} else {
		return TransformBinaryOperator(name, move(left_expr), move(right_expr));
	}
}
