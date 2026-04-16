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
#include "duckdb/parser/expression/columnref_expression.hpp"
#include "duckdb/parser/tableref/emptytableref.hpp"
#include "duckdb/parser/tableref/table_function_ref.hpp"
#include "duckdb/parser/parser_options.hpp"
#include "duckdb/parser/transformer.hpp"

namespace duckdb {

unique_ptr<ParsedExpression> Transformer::TransformUnaryOperator(const string &op, unique_ptr<ParsedExpression> child) {
	vector<unique_ptr<ParsedExpression>> children;
	children.push_back(std::move(child));

	// built-in operator function
	auto result = make_uniq<FunctionExpression>(op, std::move(children));
	result->is_operator = true;
	return std::move(result);
}

unique_ptr<ParsedExpression> Transformer::TransformBinaryOperator(string op, unique_ptr<ParsedExpression> left,
                                                                  unique_ptr<ParsedExpression> right) {
	vector<unique_ptr<ParsedExpression>> children;
	children.push_back(std::move(left));
	children.push_back(std::move(right));

	if (options.integer_division && op == "/") {
		op = "//";
	}
	if (op == "#>" || op == "#>>") {
		// TODO: this looks bad to be honest because we register pg_json_extract_path / pg_json_extract_path_text
		// in sdb
		// PG json path operators: json #> text[] / json #>> text[]
		auto func_name = (op == "#>") ? "pg_json_extract_path" : "pg_json_extract_path_text";
		auto result = make_uniq<FunctionExpression>(func_name, std::move(children));
		result->is_operator = true;
		return std::move(result);
	} else if (op == "~" || op == "!~" || op == "~*" || op == "!~*") {
		// rewrite regex operators into regexp_full_match
		// ~* / !~* are case-insensitive variants
		bool invert = (op == "!~" || op == "!~*");
		bool case_insensitive = (op == "~*" || op == "!~*");

		if (case_insensitive) {
			children.push_back(make_uniq<ConstantExpression>(Value("i")));
		}
		auto result = make_uniq<FunctionExpression>("regexp_full_match", std::move(children));
		result->is_operator = true; // PG: these are operators, column name should be ?column?
		if (invert) {
			return make_uniq<OperatorExpression>(ExpressionType::OPERATOR_NOT, std::move(result));
		} else {
			return std::move(result);
		}
	} else {
		auto target_type = OperatorToExpressionType(op);
		if (target_type != ExpressionType::INVALID) {
			// built-in comparison operator
			return make_uniq<ComparisonExpression>(target_type, std::move(children[0]), std::move(children[1]));
		}
		// not a special operator: convert to a function expression
		auto result = make_uniq<FunctionExpression>(std::move(op), std::move(children));
		result->is_operator = true;
		return std::move(result);
	}
}

unique_ptr<ParsedExpression> Transformer::TransformInExpression(const string &name, duckdb_libpgquery::PGAExpr &root) {
	auto left_expr = TransformExpression(root.lexpr);
	ExpressionType operator_type;
	// this looks very odd, but seems to be the way to find out its NOT IN
	if (name == "<>") {
		// NOT IN
		operator_type = ExpressionType::COMPARE_NOT_IN;
	} else {
		// IN
		operator_type = ExpressionType::COMPARE_IN;
	}

	if (root.rexpr->type == duckdb_libpgquery::T_PGList) {
		auto result = make_uniq<OperatorExpression>(operator_type, std::move(left_expr));
		TransformExpressionList(*PGPointerCast<duckdb_libpgquery::PGList>(root.rexpr), result->children);
		return std::move(result);
	}
	auto expr = TransformExpression(*root.rexpr);

	vector<unique_ptr<ParsedExpression>> children;
	children.push_back(std::move(expr));
	children.push_back(std::move(left_expr));
	auto result = make_uniq_base<ParsedExpression, FunctionExpression>("contains", std::move(children));
	if (operator_type == ExpressionType::COMPARE_NOT_IN) {
		result = make_uniq_base<ParsedExpression, OperatorExpression>(ExpressionType::OPERATOR_NOT, std::move(result));
	}
	return result;
}

unique_ptr<ParsedExpression> Transformer::TransformAExprInternal(duckdb_libpgquery::PGAExpr &root) {
	auto name = string(PGPointerCast<duckdb_libpgquery::PGValue>(root.name->head->data.ptr_value)->val.str);

	switch (root.kind) {
	case duckdb_libpgquery::PG_AEXPR_OP_ALL:
	case duckdb_libpgquery::PG_AEXPR_OP_ANY: {
		// left=ANY(right)
		// we turn this into left=ANY((SELECT UNNEST(right)))
		auto left_expr = TransformExpression(root.lexpr);
		auto right_expr = TransformExpression(root.rexpr);

		// TODO(mbkkt) move this to binder, but right now in binder is generic unnest
		if (right_expr->expression_class == ExpressionClass::CONSTANT) {
			auto &constant = right_expr->Cast<ConstantExpression>();
			if (constant.value.type() == LogicalType::VARCHAR) {
				auto elem_type = [&] -> LogicalType {
					if (left_expr->expression_class == ExpressionClass::CONSTANT) {
						return left_expr->Cast<ConstantExpression>().value.type();
					} else if (left_expr->expression_class == ExpressionClass::CAST) {
						return left_expr->Cast<CastExpression>().cast_type;
					} else {
						return LogicalType::VARCHAR;
					}
				}();
				right_expr = make_uniq<CastExpression>(LogicalType::LIST(elem_type), std::move(right_expr));
			}
		}

		auto subquery_expr = make_uniq<SubqueryExpression>();
		auto select_statement = make_uniq<SelectStatement>();
		auto select_node = make_uniq<SelectNode>();
		vector<unique_ptr<ParsedExpression>> children;
		children.push_back(std::move(right_expr));

		select_node->select_list.push_back(make_uniq<FunctionExpression>("UNNEST", std::move(children)));
		select_node->from_table = make_uniq<EmptyTableRef>();
		select_statement->node = std::move(select_node);
		subquery_expr->subquery = std::move(select_statement);
		subquery_expr->subquery_type = SubqueryType::ANY;
		subquery_expr->child = std::move(left_expr);
		subquery_expr->comparison_type = OperatorToExpressionType(name);
		SetQueryLocation(*subquery_expr, root.location);
		if (subquery_expr->comparison_type == ExpressionType::INVALID) {
			// For LIKE/ILIKE operators (~~, ~~*, !~~, !~~*) in ANY/ALL,
			// rewrite to EXISTS/NOT EXISTS with WHERE clause since
			// DuckDB subquery ANY/ALL only supports comparison operators.
			// x ~~* ANY(arr) -> EXISTS (SELECT FROM UNNEST(arr) t(v) WHERE x ILIKE v)
			// x ~~* ALL(arr) -> NOT EXISTS (SELECT FROM UNNEST(arr) t(v) WHERE NOT (x ILIKE v))
			bool is_like = (name == "~~");
			bool is_ilike = (name == "~~*");
			bool is_not_like = (name == "!~~");
			bool is_not_ilike = (name == "!~~*");
			if (is_like || is_ilike || is_not_like || is_not_ilike) {
				// Recover lhs (was moved into subquery_expr->child)
				auto lhs = std::move(subquery_expr->child);
				// Recover rhs array (from UNNEST args in the existing subquery)
				auto &existing_select = subquery_expr->subquery->node->Cast<SelectNode>();
				auto &unnest_func = existing_select.select_list[0]->Cast<FunctionExpression>();
				auto rhs_array = std::move(unnest_func.children[0]);

				// Build: EXISTS (SELECT 1 FROM UNNEST(arr) AS t(v) WHERE lhs ILIKE v)
				auto new_select = make_uniq<SelectNode>();
				new_select->select_list.push_back(make_uniq<ConstantExpression>(Value::INTEGER(1)));

				// FROM UNNEST(arr) AS t(v)
				auto table_func = make_uniq<TableFunctionRef>();
				vector<unique_ptr<ParsedExpression>> unnest_args;
				unnest_args.push_back(std::move(rhs_array));
				table_func->function = make_uniq<FunctionExpression>("UNNEST", std::move(unnest_args));
				table_func->alias = "t";
				table_func->column_name_alias.push_back("v");
				new_select->from_table = std::move(table_func);

				// WHERE lhs LIKE/ILIKE v
				string func_name = (is_ilike || is_not_ilike) ? "ilike_escape" : "like_escape";
				vector<unique_ptr<ParsedExpression>> like_children;
				like_children.push_back(std::move(lhs));
				like_children.push_back(make_uniq<ColumnRefExpression>("v"));
				like_children.push_back(make_uniq<ConstantExpression>(Value("\\")));
				unique_ptr<ParsedExpression> where_cond =
				    make_uniq<FunctionExpression>(func_name, std::move(like_children));

				if (is_not_like || is_not_ilike) {
					where_cond = make_uniq<OperatorExpression>(ExpressionType::OPERATOR_NOT, std::move(where_cond));
				}

				bool is_all = (root.kind == duckdb_libpgquery::PG_AEXPR_OP_ALL);
				if (is_all) {
					where_cond = make_uniq<OperatorExpression>(ExpressionType::OPERATOR_NOT, std::move(where_cond));
				}
				new_select->where_clause = std::move(where_cond);

				auto new_statement = make_uniq<SelectStatement>();
				new_statement->node = std::move(new_select);

				auto exists_expr = make_uniq<SubqueryExpression>();
				exists_expr->subquery = std::move(new_statement);
				exists_expr->subquery_type = SubqueryType::EXISTS;
				SetQueryLocation(*exists_expr, root.location);

				if (is_all) {
					return make_uniq<OperatorExpression>(ExpressionType::OPERATOR_NOT, std::move(exists_expr));
				}
				return std::move(exists_expr);
			}
			throw ParserException("Unsupported comparison \"%s\" for ANY/ALL subquery", name);
		}

		if (root.kind == duckdb_libpgquery::PG_AEXPR_OP_ALL) {
			// ALL sublink is equivalent to NOT(ANY) with inverted comparison
			// e.g. [= ALL()] is equivalent to [NOT(<> ANY())]
			// first invert the comparison type
			subquery_expr->comparison_type = NegateComparisonExpression(subquery_expr->comparison_type);
			return make_uniq<OperatorExpression>(ExpressionType::OPERATOR_NOT, std::move(subquery_expr));
		}
		return std::move(subquery_expr);
	}
	case duckdb_libpgquery::PG_AEXPR_IN: {
		return TransformInExpression(name, root);
	}
	// rewrite NULLIF(a, b) into CASE WHEN a=b THEN NULL ELSE a END
	case duckdb_libpgquery::PG_AEXPR_NULLIF: {
		vector<unique_ptr<ParsedExpression>> children;
		children.push_back(TransformExpression(root.lexpr));
		children.push_back(TransformExpression(root.rexpr));
		return make_uniq<FunctionExpression>("nullif", std::move(children));
	}
	// rewrite (NOT) X BETWEEN A AND B into (NOT) AND(GREATERTHANOREQUALTO(X,
	// A), LESSTHANOREQUALTO(X, B))
	case duckdb_libpgquery::PG_AEXPR_BETWEEN:
	case duckdb_libpgquery::PG_AEXPR_NOT_BETWEEN: {
		auto between_args = PGPointerCast<duckdb_libpgquery::PGList>(root.rexpr);
		if (between_args->length != 2 || !between_args->head->data.ptr_value || !between_args->tail->data.ptr_value) {
			throw InternalException("(NOT) BETWEEN needs two args");
		}

		auto input = TransformExpression(root.lexpr);
		auto between_left =
		    TransformExpression(PGPointerCast<duckdb_libpgquery::PGNode>(between_args->head->data.ptr_value));
		auto between_right =
		    TransformExpression(PGPointerCast<duckdb_libpgquery::PGNode>(between_args->tail->data.ptr_value));

		auto compare_between =
		    make_uniq<BetweenExpression>(std::move(input), std::move(between_left), std::move(between_right));
		if (root.kind == duckdb_libpgquery::PG_AEXPR_BETWEEN) {
			return std::move(compare_between);
		} else {
			return make_uniq<OperatorExpression>(ExpressionType::OPERATOR_NOT, std::move(compare_between));
		}
	}
	// rewrite (NOT) X BETWEEN SYMMETRIC A AND B into
	// (NOT) BETWEEN(X, LEAST(A, B), GREATEST(A, B))
	case duckdb_libpgquery::PG_AEXPR_BETWEEN_SYM:
	case duckdb_libpgquery::PG_AEXPR_NOT_BETWEEN_SYM: {
		auto between_args = PGPointerCast<duckdb_libpgquery::PGList>(root.rexpr);
		if (between_args->length != 2 || !between_args->head->data.ptr_value || !between_args->tail->data.ptr_value) {
			throw InternalException("(NOT) BETWEEN SYMMETRIC needs two args");
		}

		auto input = TransformExpression(root.lexpr);
		auto left = TransformExpression(PGPointerCast<duckdb_libpgquery::PGNode>(between_args->head->data.ptr_value));
		auto right = TransformExpression(PGPointerCast<duckdb_libpgquery::PGNode>(between_args->tail->data.ptr_value));

		vector<unique_ptr<ParsedExpression>> least_args;
		least_args.push_back(left->Copy());
		least_args.push_back(right->Copy());
		auto least_expr = make_uniq<FunctionExpression>("least", std::move(least_args));

		vector<unique_ptr<ParsedExpression>> greatest_args;
		greatest_args.push_back(std::move(left));
		greatest_args.push_back(std::move(right));
		auto greatest_expr = make_uniq<FunctionExpression>("greatest", std::move(greatest_args));

		auto compare_between =
		    make_uniq<BetweenExpression>(std::move(input), std::move(least_expr), std::move(greatest_expr));
		if (root.kind == duckdb_libpgquery::PG_AEXPR_BETWEEN_SYM) {
			return std::move(compare_between);
		} else {
			return make_uniq<OperatorExpression>(ExpressionType::OPERATOR_NOT, std::move(compare_between));
		}
	}
	// rewrite SIMILAR TO into regexp_full_match('asdf', '.*sd.*')
	case duckdb_libpgquery::PG_AEXPR_SIMILAR: {
		auto left_expr = TransformExpression(root.lexpr);
		auto right_expr = TransformExpression(root.rexpr);

		vector<unique_ptr<ParsedExpression>> children;
		children.push_back(std::move(left_expr));

		auto &similar_func = right_expr->Cast<FunctionExpression>();
		D_ASSERT(similar_func.function_name == "similar_escape");
		D_ASSERT(similar_func.children.size() == 2);
		// wrap the pattern with similar_to_escape so %->.*  _->. conversion happens
		vector<unique_ptr<ParsedExpression>> escape_args;
		escape_args.push_back(std::move(similar_func.children[0]));
		// pass escape char through if provided (non-NULL constant)
		if (similar_func.children[1]->GetExpressionType() == ExpressionType::VALUE_CONSTANT) {
			auto &constant = similar_func.children[1]->Cast<ConstantExpression>();
			if (!constant.value.IsNull()) {
				escape_args.push_back(std::move(similar_func.children[1]));
			}
		} else {
			escape_args.push_back(std::move(similar_func.children[1]));
		}
		children.push_back(make_uniq<FunctionExpression>("similar_to_escape", std::move(escape_args)));

		// this looks very odd, but seems to be the way to find out its NOT IN
		bool invert_similar = false;
		if (name == "!~") {
			// NOT SIMILAR TO
			invert_similar = true;
		}
		const auto regex_function = "regexp_full_match";
		auto result = make_uniq<FunctionExpression>(regex_function, std::move(children));
		result->is_operator = true; // PG: SIMILAR TO is an operator, column name should be ?column?

		if (invert_similar) {
			return make_uniq<OperatorExpression>(ExpressionType::OPERATOR_NOT, std::move(result));
		} else {
			return std::move(result);
		}
	}
	case duckdb_libpgquery::PG_AEXPR_NOT_DISTINCT: {
		auto left_expr = TransformExpression(root.lexpr);
		auto right_expr = TransformExpression(root.rexpr);
		return make_uniq<ComparisonExpression>(ExpressionType::COMPARE_NOT_DISTINCT_FROM, std::move(left_expr),
		                                       std::move(right_expr));
	}
	case duckdb_libpgquery::PG_AEXPR_DISTINCT: {
		auto left_expr = TransformExpression(root.lexpr);
		auto right_expr = TransformExpression(root.rexpr);
		return make_uniq<ComparisonExpression>(ExpressionType::COMPARE_DISTINCT_FROM, std::move(left_expr),
		                                       std::move(right_expr));
	}

	default:
		break;
	}
	auto left_expr = TransformExpression(root.lexpr);
	auto right_expr = TransformExpression(root.rexpr);

	if (!left_expr) {
		// prefix operator
		return TransformUnaryOperator(name, std::move(right_expr));
	} else if (!right_expr) {
		// postfix operator, only ! is currently supported
		return TransformUnaryOperator(name + "__postfix", std::move(left_expr));
	} else {
		return TransformBinaryOperator(std::move(name), std::move(left_expr), std::move(right_expr));
	}
}

unique_ptr<ParsedExpression> Transformer::TransformAExpr(duckdb_libpgquery::PGAExpr &root) {
	auto result = TransformAExprInternal(root);
	if (result) {
		SetQueryLocation(*result, root.location);
	}
	return result;
}

} // namespace duckdb
