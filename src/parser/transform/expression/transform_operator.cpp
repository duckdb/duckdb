#include "duckdb/parser/expression/case_expression.hpp"
#include "duckdb/parser/expression/cast_expression.hpp"
#include "duckdb/parser/expression/comparison_expression.hpp"
#include "duckdb/parser/expression/conjunction_expression.hpp"
#include "duckdb/parser/expression/constant_expression.hpp"
#include "duckdb/parser/expression/function_expression.hpp"
#include "duckdb/parser/expression/operator_expression.hpp"
#include "duckdb/parser/transformer.hpp"

namespace duckdb {

ExpressionType Transformer::OperatorToExpressionType(const string &op) {
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

unique_ptr<ParsedExpression> Transformer::TransformUnaryOperator(const string &op, duckdb_libpgquery::PGNode *child) {
	D_ASSERT(child);

	const auto schema = DEFAULT_SCHEMA;

	vector<unique_ptr<ParsedExpression>> children;

	// built-in operator function
	auto result = make_unique<FunctionExpression>(schema, op, children);
	result->children.resize(1);
	TransformExpressionLazy(child, result->children[0]);

	result->is_operator = true;
	return move(result);
}

unique_ptr<ParsedExpression> Transformer::TransformBinaryOperator(const string &op, duckdb_libpgquery::PGNode *left,
                                                                  duckdb_libpgquery::PGNode *right) {
	const auto schema = DEFAULT_SCHEMA;

	vector<unique_ptr<ParsedExpression>> children;

	if (op == "~" || op == "!~") {
		// rewrite 'asdf' SIMILAR TO '.*sd.*' into regexp_full_match('asdf', '.*sd.*')
		bool invert_similar = op == "!~";

		auto result = make_unique<FunctionExpression>(schema, "regexp_full_match", children);
		result->children.resize(2);

		TransformExpressionLazy(left, result->children[0]);
		TransformExpressionLazy(right, result->children[1]);
		if (invert_similar) {
			return make_unique<OperatorExpression>(ExpressionType::OPERATOR_NOT, move(result));
		} else {
			return move(result);
		}
	} else {
		auto target_type = OperatorToExpressionType(op);
		if (target_type != ExpressionType::INVALID) {
			// built-in comparison operator
			auto comparison = make_unique<ComparisonExpression>(target_type);
			TransformExpressionLazy(left, comparison->left);
			TransformExpressionLazy(right, comparison->right);
			return move(comparison);
		}
		// not a special operator: convert to a function expression
		auto result = make_unique<FunctionExpression>(schema, op, children);
		result->is_operator = true;
		result->children.resize(2);

		TransformExpressionLazy(left, result->children[0]);
		TransformExpressionLazy(right, result->children[1]);
		return move(result);
	}
}

unique_ptr<ParsedExpression> Transformer::TransformAExpr(duckdb_libpgquery::PGAExpr *root) {
	if (!root) {
		return nullptr;
	}
	auto name = string((reinterpret_cast<duckdb_libpgquery::PGValue *>(root->name->head->data.ptr_value))->val.str);

	switch (root->kind) {
	case duckdb_libpgquery::PG_AEXPR_IN: {
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
		TransformExpressionList((duckdb_libpgquery::PGList *)root->rexpr, result->children);
		return move(result);
	}
	// rewrite NULLIF(a, b) into CASE WHEN a=b THEN NULL ELSE a END
	case duckdb_libpgquery::PG_AEXPR_NULLIF: {
		vector<unique_ptr<ParsedExpression>> children;
		children.push_back(TransformExpression(root->lexpr));
		children.push_back(TransformExpression(root->rexpr));
		return make_unique<FunctionExpression>("nullif", children);
	}
	// rewrite (NOT) X BETWEEN A AND B into (NOT) AND(GREATERTHANOREQUALTO(X,
	// A), LESSTHANOREQUALTO(X, B))
	case duckdb_libpgquery::PG_AEXPR_BETWEEN:
	case duckdb_libpgquery::PG_AEXPR_NOT_BETWEEN: {
		auto between_args = reinterpret_cast<duckdb_libpgquery::PGList *>(root->rexpr);
		if (between_args->length != 2 || !between_args->head->data.ptr_value || !between_args->tail->data.ptr_value) {
			throw Exception("(NOT) BETWEEN needs two args");
		}

		auto between_left =
		    TransformExpression(reinterpret_cast<duckdb_libpgquery::PGNode *>(between_args->head->data.ptr_value));
		auto between_right =
		    TransformExpression(reinterpret_cast<duckdb_libpgquery::PGNode *>(between_args->tail->data.ptr_value));

		auto compare_left = make_unique<ComparisonExpression>(ExpressionType::COMPARE_GREATERTHANOREQUALTO,
		                                                      TransformExpression(root->lexpr), move(between_left));
		auto compare_right = make_unique<ComparisonExpression>(ExpressionType::COMPARE_LESSTHANOREQUALTO,
		                                                       TransformExpression(root->lexpr), move(between_right));
		auto compare_between = make_unique<ConjunctionExpression>(ExpressionType::CONJUNCTION_AND, move(compare_left),
		                                                          move(compare_right));
		if (root->kind == duckdb_libpgquery::PG_AEXPR_BETWEEN) {
			return move(compare_between);
		} else {
			return make_unique<OperatorExpression>(ExpressionType::OPERATOR_NOT, move(compare_between));
		}
	}
	// rewrite SIMILAR TO into regexp_full_match('asdf', '.*sd.*')
	case duckdb_libpgquery::PG_AEXPR_SIMILAR: {
		auto left_expr = TransformExpression(root->lexpr);
		auto right_expr = TransformExpression(root->rexpr);

		vector<unique_ptr<ParsedExpression>> children;
		children.push_back(move(left_expr));

		auto &similar_func = reinterpret_cast<FunctionExpression &>(*right_expr);
		D_ASSERT(similar_func.function_name == "similar_escape");
		D_ASSERT(similar_func.children.size() == 2);
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
	}
	case duckdb_libpgquery::PG_AEXPR_NOT_DISTINCT: {
		auto left_expr = TransformExpression(root->lexpr);
		auto right_expr = TransformExpression(root->rexpr);
		return make_unique<ComparisonExpression>(ExpressionType::COMPARE_NOT_DISTINCT_FROM, move(left_expr),
		                                         move(right_expr));
	}
	case duckdb_libpgquery::PG_AEXPR_DISTINCT: {
		auto left_expr = TransformExpression(root->lexpr);
		auto right_expr = TransformExpression(root->rexpr);
		return make_unique<ComparisonExpression>(ExpressionType::COMPARE_DISTINCT_FROM, move(left_expr),
		                                         move(right_expr));
	}
	default:
		break;
	}
	if (!root->lexpr) {
		// prefix operator
		return TransformUnaryOperator(name, root->rexpr);
	} else if (!root->rexpr) {
		// postfix operator, only ! is currently supported
		return TransformUnaryOperator(name + "__postfix", root->lexpr);
	} else {
		return TransformBinaryOperator(name, root->lexpr, root->rexpr);
	}
}

} // namespace duckdb
