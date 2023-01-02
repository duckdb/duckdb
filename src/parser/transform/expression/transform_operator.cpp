#include "duckdb/parser/expression/between_expression.hpp"
#include "duckdb/parser/expression/case_expression.hpp"
#include "duckdb/parser/expression/cast_expression.hpp"
#include "duckdb/parser/expression/comparison_expression.hpp"
#include "duckdb/parser/expression/conjunction_expression.hpp"
#include "duckdb/parser/expression/constant_expression.hpp"
#include "duckdb/parser/expression/function_expression.hpp"
#include "duckdb/parser/expression/operator_expression.hpp"
#include "duckdb/parser/expression/subquery_expression.hpp"
#include "duckdb/parser/statement/select_statement.hpp"
#include "duckdb/parser/query_node/select_node.hpp"
#include "duckdb/parser/tableref/emptytableref.hpp"
#include "duckdb/parser/transformer.hpp"

namespace duckdb {

unique_ptr<ParsedExpression> Transformer::TransformUnaryOperator(const string &op, unique_ptr<ParsedExpression> child) {
	vector<unique_ptr<ParsedExpression>> children;
	children.push_back(std::move(child));

	// built-in operator function
	auto result = make_unique<FunctionExpression>(op, std::move(children));
	result->is_operator = true;
	return std::move(result);
}

unique_ptr<ParsedExpression> Transformer::TransformBinaryOperator(const string &op, unique_ptr<ParsedExpression> left,
                                                                  unique_ptr<ParsedExpression> right) {
	vector<unique_ptr<ParsedExpression>> children;
	children.push_back(std::move(left));
	children.push_back(std::move(right));

	if (op == "~" || op == "!~") {
		// rewrite 'asdf' SIMILAR TO '.*sd.*' into regexp_full_match('asdf', '.*sd.*')
		bool invert_similar = op == "!~";

		auto result = make_unique<FunctionExpression>("regexp_full_match", std::move(children));
		if (invert_similar) {
			return make_unique<OperatorExpression>(ExpressionType::OPERATOR_NOT, std::move(result));
		} else {
			return std::move(result);
		}
	} else {
		auto target_type = OperatorToExpressionType(op);
		if (target_type != ExpressionType::INVALID) {
			// built-in comparison operator
			return make_unique<ComparisonExpression>(target_type, std::move(children[0]), std::move(children[1]));
		}
		// not a special operator: convert to a function expression
		auto result = make_unique<FunctionExpression>(op, std::move(children));
		result->is_operator = true;
		return std::move(result);
	}
}

unique_ptr<ParsedExpression> Transformer::TransformAExprInternal(duckdb_libpgquery::PGAExpr *root) {
	D_ASSERT(root);
	auto name = string((reinterpret_cast<duckdb_libpgquery::PGValue *>(root->name->head->data.ptr_value))->val.str);

	switch (root->kind) {
	case duckdb_libpgquery::PG_AEXPR_OP_ALL:
	case duckdb_libpgquery::PG_AEXPR_OP_ANY: {
		// left=ANY(right)
		// we turn this into left=ANY((SELECT UNNEST(right)))
		auto left_expr = TransformExpression(root->lexpr);
		auto right_expr = TransformExpression(root->rexpr);

		auto subquery_expr = make_unique<SubqueryExpression>();
		auto select_statement = make_unique<SelectStatement>();
		auto select_node = make_unique<SelectNode>();
		vector<unique_ptr<ParsedExpression>> children;
		children.push_back(std::move(right_expr));

		select_node->select_list.push_back(make_unique<FunctionExpression>("UNNEST", std::move(children)));
		select_node->from_table = make_unique<EmptyTableRef>();
		select_statement->node = std::move(select_node);
		subquery_expr->subquery = std::move(select_statement);
		subquery_expr->subquery_type = SubqueryType::ANY;
		subquery_expr->child = std::move(left_expr);
		subquery_expr->comparison_type = OperatorToExpressionType(name);
		subquery_expr->query_location = root->location;

		if (root->kind == duckdb_libpgquery::PG_AEXPR_OP_ALL) {
			// ALL sublink is equivalent to NOT(ANY) with inverted comparison
			// e.g. [= ALL()] is equivalent to [NOT(<> ANY())]
			// first invert the comparison type
			subquery_expr->comparison_type = NegateComparisionExpression(subquery_expr->comparison_type);
			return make_unique<OperatorExpression>(ExpressionType::OPERATOR_NOT, std::move(subquery_expr));
		}
		return std::move(subquery_expr);
	}
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
		auto result = make_unique<OperatorExpression>(operator_type, std::move(left_expr));
		result->query_location = root->location;
		TransformExpressionList(*((duckdb_libpgquery::PGList *)root->rexpr), result->children);
		return std::move(result);
	}
	// rewrite NULLIF(a, b) into CASE WHEN a=b THEN NULL ELSE a END
	case duckdb_libpgquery::PG_AEXPR_NULLIF: {
		vector<unique_ptr<ParsedExpression>> children;
		children.push_back(TransformExpression(root->lexpr));
		children.push_back(TransformExpression(root->rexpr));
		return make_unique<FunctionExpression>("nullif", std::move(children));
	}
	// rewrite (NOT) X BETWEEN A AND B into (NOT) AND(GREATERTHANOREQUALTO(X,
	// A), LESSTHANOREQUALTO(X, B))
	case duckdb_libpgquery::PG_AEXPR_BETWEEN:
	case duckdb_libpgquery::PG_AEXPR_NOT_BETWEEN: {
		auto between_args = reinterpret_cast<duckdb_libpgquery::PGList *>(root->rexpr);
		if (between_args->length != 2 || !between_args->head->data.ptr_value || !between_args->tail->data.ptr_value) {
			throw InternalException("(NOT) BETWEEN needs two args");
		}

		auto input = TransformExpression(root->lexpr);
		auto between_left =
		    TransformExpression(reinterpret_cast<duckdb_libpgquery::PGNode *>(between_args->head->data.ptr_value));
		auto between_right =
		    TransformExpression(reinterpret_cast<duckdb_libpgquery::PGNode *>(between_args->tail->data.ptr_value));

		auto compare_between =
		    make_unique<BetweenExpression>(std::move(input), std::move(between_left), std::move(between_right));
		if (root->kind == duckdb_libpgquery::PG_AEXPR_BETWEEN) {
			return std::move(compare_between);
		} else {
			return make_unique<OperatorExpression>(ExpressionType::OPERATOR_NOT, std::move(compare_between));
		}
	}
	// rewrite SIMILAR TO into regexp_full_match('asdf', '.*sd.*')
	case duckdb_libpgquery::PG_AEXPR_SIMILAR: {
		auto left_expr = TransformExpression(root->lexpr);
		auto right_expr = TransformExpression(root->rexpr);

		vector<unique_ptr<ParsedExpression>> children;
		children.push_back(std::move(left_expr));

		auto &similar_func = reinterpret_cast<FunctionExpression &>(*right_expr);
		D_ASSERT(similar_func.function_name == "similar_escape");
		D_ASSERT(similar_func.children.size() == 2);
		if (similar_func.children[1]->type != ExpressionType::VALUE_CONSTANT) {
			throw NotImplementedException("Custom escape in SIMILAR TO");
		}
		auto &constant = (ConstantExpression &)*similar_func.children[1];
		if (!constant.value.IsNull()) {
			throw NotImplementedException("Custom escape in SIMILAR TO");
		}
		// take the child of the similar_func
		children.push_back(std::move(similar_func.children[0]));

		// this looks very odd, but seems to be the way to find out its NOT IN
		bool invert_similar = false;
		if (name == "!~") {
			// NOT SIMILAR TO
			invert_similar = true;
		}
		const auto regex_function = "regexp_full_match";
		auto result = make_unique<FunctionExpression>(regex_function, std::move(children));

		if (invert_similar) {
			return make_unique<OperatorExpression>(ExpressionType::OPERATOR_NOT, std::move(result));
		} else {
			return std::move(result);
		}
	}
	case duckdb_libpgquery::PG_AEXPR_NOT_DISTINCT: {
		auto left_expr = TransformExpression(root->lexpr);
		auto right_expr = TransformExpression(root->rexpr);
		return make_unique<ComparisonExpression>(ExpressionType::COMPARE_NOT_DISTINCT_FROM, std::move(left_expr),
		                                         std::move(right_expr));
	}
	case duckdb_libpgquery::PG_AEXPR_DISTINCT: {
		auto left_expr = TransformExpression(root->lexpr);
		auto right_expr = TransformExpression(root->rexpr);
		return make_unique<ComparisonExpression>(ExpressionType::COMPARE_DISTINCT_FROM, std::move(left_expr),
		                                         std::move(right_expr));
	}

	default:
		break;
	}
	auto left_expr = TransformExpression(root->lexpr);
	auto right_expr = TransformExpression(root->rexpr);

	if (!left_expr) {
		// prefix operator
		return TransformUnaryOperator(name, std::move(right_expr));
	} else if (!right_expr) {
		// postfix operator, only ! is currently supported
		return TransformUnaryOperator(name + "__postfix", std::move(left_expr));
	} else {
		return TransformBinaryOperator(name, std::move(left_expr), std::move(right_expr));
	}
}

unique_ptr<ParsedExpression> Transformer::TransformAExpr(duckdb_libpgquery::PGAExpr *root) {
	auto result = TransformAExprInternal(root);
	if (result) {
		result->query_location = root->location;
	}
	return result;
}

} // namespace duckdb
