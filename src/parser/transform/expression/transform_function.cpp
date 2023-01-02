#include "duckdb/common/string_util.hpp"
#include "duckdb/common/to_string.hpp"
#include "duckdb/parser/expression/case_expression.hpp"
#include "duckdb/parser/expression/cast_expression.hpp"
#include "duckdb/parser/expression/function_expression.hpp"

#include "duckdb/parser/expression/operator_expression.hpp"
#include "duckdb/parser/expression/star_expression.hpp"
#include "duckdb/parser/expression/window_expression.hpp"
#include "duckdb/parser/transformer.hpp"

namespace duckdb {

static ExpressionType WindowToExpressionType(string &fun_name) {
	if (fun_name == "rank") {
		return ExpressionType::WINDOW_RANK;
	} else if (fun_name == "rank_dense" || fun_name == "dense_rank") {
		return ExpressionType::WINDOW_RANK_DENSE;
	} else if (fun_name == "percent_rank") {
		return ExpressionType::WINDOW_PERCENT_RANK;
	} else if (fun_name == "row_number") {
		return ExpressionType::WINDOW_ROW_NUMBER;
	} else if (fun_name == "first_value" || fun_name == "first") {
		return ExpressionType::WINDOW_FIRST_VALUE;
	} else if (fun_name == "last_value" || fun_name == "last") {
		return ExpressionType::WINDOW_LAST_VALUE;
	} else if (fun_name == "nth_value" || fun_name == "last") {
		return ExpressionType::WINDOW_NTH_VALUE;
	} else if (fun_name == "cume_dist") {
		return ExpressionType::WINDOW_CUME_DIST;
	} else if (fun_name == "lead") {
		return ExpressionType::WINDOW_LEAD;
	} else if (fun_name == "lag") {
		return ExpressionType::WINDOW_LAG;
	} else if (fun_name == "ntile") {
		return ExpressionType::WINDOW_NTILE;
	}

	return ExpressionType::WINDOW_AGGREGATE;
}

void Transformer::TransformWindowDef(duckdb_libpgquery::PGWindowDef *window_spec, WindowExpression *expr) {
	D_ASSERT(window_spec);
	D_ASSERT(expr);

	// next: partitioning/ordering expressions
	if (window_spec->partitionClause) {
		TransformExpressionList(*window_spec->partitionClause, expr->partitions);
	}
	TransformOrderBy(window_spec->orderClause, expr->orders);
}

void Transformer::TransformWindowFrame(duckdb_libpgquery::PGWindowDef *window_spec, WindowExpression *expr) {
	D_ASSERT(window_spec);
	D_ASSERT(expr);

	// finally: specifics of bounds
	expr->start_expr = TransformExpression(window_spec->startOffset);
	expr->end_expr = TransformExpression(window_spec->endOffset);

	if ((window_spec->frameOptions & FRAMEOPTION_END_UNBOUNDED_PRECEDING) ||
	    (window_spec->frameOptions & FRAMEOPTION_START_UNBOUNDED_FOLLOWING)) {
		throw InternalException(
		    "Window frames starting with unbounded following or ending in unbounded preceding make no sense");
	}

	const bool rangeMode = (window_spec->frameOptions & FRAMEOPTION_RANGE) != 0;
	if (window_spec->frameOptions & FRAMEOPTION_START_UNBOUNDED_PRECEDING) {
		expr->start = WindowBoundary::UNBOUNDED_PRECEDING;
	} else if (window_spec->frameOptions & FRAMEOPTION_START_VALUE_PRECEDING) {
		expr->start = rangeMode ? WindowBoundary::EXPR_PRECEDING_RANGE : WindowBoundary::EXPR_PRECEDING_ROWS;
	} else if (window_spec->frameOptions & FRAMEOPTION_START_VALUE_FOLLOWING) {
		expr->start = rangeMode ? WindowBoundary::EXPR_FOLLOWING_RANGE : WindowBoundary::EXPR_FOLLOWING_ROWS;
	} else if (window_spec->frameOptions & FRAMEOPTION_START_CURRENT_ROW) {
		expr->start = rangeMode ? WindowBoundary::CURRENT_ROW_RANGE : WindowBoundary::CURRENT_ROW_ROWS;
	}

	if (window_spec->frameOptions & FRAMEOPTION_END_UNBOUNDED_FOLLOWING) {
		expr->end = WindowBoundary::UNBOUNDED_FOLLOWING;
	} else if (window_spec->frameOptions & FRAMEOPTION_END_VALUE_PRECEDING) {
		expr->end = rangeMode ? WindowBoundary::EXPR_PRECEDING_RANGE : WindowBoundary::EXPR_PRECEDING_ROWS;
	} else if (window_spec->frameOptions & FRAMEOPTION_END_VALUE_FOLLOWING) {
		expr->end = rangeMode ? WindowBoundary::EXPR_FOLLOWING_RANGE : WindowBoundary::EXPR_FOLLOWING_ROWS;
	} else if (window_spec->frameOptions & FRAMEOPTION_END_CURRENT_ROW) {
		expr->end = rangeMode ? WindowBoundary::CURRENT_ROW_RANGE : WindowBoundary::CURRENT_ROW_ROWS;
	}

	D_ASSERT(expr->start != WindowBoundary::INVALID && expr->end != WindowBoundary::INVALID);
	if (((window_spec->frameOptions & (FRAMEOPTION_START_VALUE_PRECEDING | FRAMEOPTION_START_VALUE_FOLLOWING)) &&
	     !expr->start_expr) ||
	    ((window_spec->frameOptions & (FRAMEOPTION_END_VALUE_PRECEDING | FRAMEOPTION_END_VALUE_FOLLOWING)) &&
	     !expr->end_expr)) {
		throw InternalException("Failed to transform window boundary expression");
	}
}

unique_ptr<ParsedExpression> Transformer::TransformFuncCall(duckdb_libpgquery::PGFuncCall *root) {
	auto name = root->funcname;
	string catalog, schema, function_name;
	if (name->length == 3) {
		// catalog + schema + name
		catalog = reinterpret_cast<duckdb_libpgquery::PGValue *>(name->head->data.ptr_value)->val.str;
		schema = reinterpret_cast<duckdb_libpgquery::PGValue *>(name->head->next->data.ptr_value)->val.str;
		function_name = reinterpret_cast<duckdb_libpgquery::PGValue *>(name->head->next->next->data.ptr_value)->val.str;
	} else if (name->length == 2) {
		// schema + name
		catalog = INVALID_CATALOG;
		schema = reinterpret_cast<duckdb_libpgquery::PGValue *>(name->head->data.ptr_value)->val.str;
		function_name = reinterpret_cast<duckdb_libpgquery::PGValue *>(name->head->next->data.ptr_value)->val.str;
	} else if (name->length == 1) {
		// unqualified name
		catalog = INVALID_CATALOG;
		schema = INVALID_SCHEMA;
		function_name = reinterpret_cast<duckdb_libpgquery::PGValue *>(name->head->data.ptr_value)->val.str;
	} else {
		throw InternalException("TransformFuncCall - Expected 1, 2 or 3 qualifications");
	}

	auto lowercase_name = StringUtil::Lower(function_name);

	if (root->over) {
		const auto win_fun_type = WindowToExpressionType(lowercase_name);
		if (win_fun_type == ExpressionType::INVALID) {
			throw InternalException("Unknown/unsupported window function");
		}

		if (root->agg_distinct) {
			throw ParserException("DISTINCT is not implemented for window functions!");
		}

		if (root->agg_order) {
			throw ParserException("ORDER BY is not implemented for window functions!");
		}

		if (win_fun_type != ExpressionType::WINDOW_AGGREGATE && root->agg_filter) {
			throw ParserException("FILTER is not implemented for non-aggregate window functions!");
		}
		if (root->export_state) {
			throw ParserException("EXPORT_STATE is not supported for window functions!");
		}

		if (win_fun_type == ExpressionType::WINDOW_AGGREGATE && root->agg_ignore_nulls) {
			throw ParserException("IGNORE NULLS is not supported for windowed aggregates");
		}

		auto expr = make_unique<WindowExpression>(win_fun_type, std::move(catalog), std::move(schema), lowercase_name);
		expr->ignore_nulls = root->agg_ignore_nulls;

		if (root->agg_filter) {
			auto filter_expr = TransformExpression(root->agg_filter);
			expr->filter_expr = std::move(filter_expr);
		}

		if (root->args) {
			vector<unique_ptr<ParsedExpression>> function_list;
			TransformExpressionList(*root->args, function_list);

			if (win_fun_type == ExpressionType::WINDOW_AGGREGATE) {
				for (auto &child : function_list) {
					expr->children.push_back(std::move(child));
				}
			} else {
				if (!function_list.empty()) {
					expr->children.push_back(std::move(function_list[0]));
				}
				if (win_fun_type == ExpressionType::WINDOW_LEAD || win_fun_type == ExpressionType::WINDOW_LAG) {
					if (function_list.size() > 1) {
						expr->offset_expr = std::move(function_list[1]);
					}
					if (function_list.size() > 2) {
						expr->default_expr = std::move(function_list[2]);
					}
					if (function_list.size() > 3) {
						throw ParserException("Incorrect number of parameters for function %s", lowercase_name);
					}
				} else if (win_fun_type == ExpressionType::WINDOW_NTH_VALUE) {
					if (function_list.size() > 1) {
						expr->children.push_back(std::move(function_list[1]));
					}
					if (function_list.size() > 2) {
						throw ParserException("Incorrect number of parameters for function %s", lowercase_name);
					}
				} else {
					if (function_list.size() > 1) {
						throw ParserException("Incorrect number of parameters for function %s", lowercase_name);
					}
				}
			}
		}
		auto window_spec = reinterpret_cast<duckdb_libpgquery::PGWindowDef *>(root->over);
		if (window_spec->name) {
			auto it = window_clauses.find(StringUtil::Lower(string(window_spec->name)));
			if (it == window_clauses.end()) {
				throw ParserException("window \"%s\" does not exist", window_spec->name);
			}
			window_spec = it->second;
			D_ASSERT(window_spec);
		}
		auto window_ref = window_spec;
		if (window_ref->refname) {
			auto it = window_clauses.find(StringUtil::Lower(string(window_spec->refname)));
			if (it == window_clauses.end()) {
				throw ParserException("window \"%s\" does not exist", window_spec->refname);
			}
			window_ref = it->second;
			D_ASSERT(window_ref);
		}
		TransformWindowDef(window_ref, expr.get());
		TransformWindowFrame(window_spec, expr.get());
		expr->query_location = root->location;
		return std::move(expr);
	}

	if (root->agg_ignore_nulls) {
		throw ParserException("IGNORE NULLS is not supported for non-window functions");
	}

	//  TransformExpressionList??
	vector<unique_ptr<ParsedExpression>> children;
	if (root->args != nullptr) {
		for (auto node = root->args->head; node != nullptr; node = node->next) {
			auto child_expr = TransformExpression((duckdb_libpgquery::PGNode *)node->data.ptr_value);
			children.push_back(std::move(child_expr));
		}
	}
	unique_ptr<ParsedExpression> filter_expr;
	if (root->agg_filter) {
		filter_expr = TransformExpression(root->agg_filter);
	}

	auto order_bys = make_unique<OrderModifier>();
	TransformOrderBy(root->agg_order, order_bys->orders);

	// Ordered aggregates can be either WITHIN GROUP or after the function arguments
	if (root->agg_within_group) {
		//	https://www.postgresql.org/docs/current/functions-aggregate.html#FUNCTIONS-ORDEREDSET-TABLE
		//  Since we implement "ordered aggregates" without sorting,
		//  we map all the ones we support to the corresponding aggregate function.
		if (order_bys->orders.size() != 1) {
			throw ParserException("Cannot use multiple ORDER BY clauses with WITHIN GROUP");
		}
		if (lowercase_name == "percentile_cont") {
			if (children.size() != 1) {
				throw ParserException("Wrong number of arguments for PERCENTILE_CONT");
			}
			lowercase_name = "quantile_cont";
		} else if (lowercase_name == "percentile_disc") {
			if (children.size() != 1) {
				throw ParserException("Wrong number of arguments for PERCENTILE_DISC");
			}
			lowercase_name = "quantile_disc";
		} else if (lowercase_name == "mode") {
			if (!children.empty()) {
				throw ParserException("Wrong number of arguments for MODE");
			}
			lowercase_name = "mode";
		} else {
			throw ParserException("Unknown ordered aggregate \"%s\".", function_name);
		}
	}

	// star gets eaten in the parser
	if (lowercase_name == "count" && children.empty()) {
		lowercase_name = "count_star";
	}

	if (lowercase_name == "if") {
		if (children.size() != 3) {
			throw ParserException("Wrong number of arguments to IF.");
		}
		auto expr = make_unique<CaseExpression>();
		CaseCheck check;
		check.when_expr = std::move(children[0]);
		check.then_expr = std::move(children[1]);
		expr->case_checks.push_back(std::move(check));
		expr->else_expr = std::move(children[2]);
		return std::move(expr);
	} else if (lowercase_name == "construct_array") {
		auto construct_array = make_unique<OperatorExpression>(ExpressionType::ARRAY_CONSTRUCTOR);
		construct_array->children = std::move(children);
		return std::move(construct_array);
	} else if (lowercase_name == "ifnull") {
		if (children.size() != 2) {
			throw ParserException("Wrong number of arguments to IFNULL.");
		}

		//  Two-argument COALESCE
		auto coalesce_op = make_unique<OperatorExpression>(ExpressionType::OPERATOR_COALESCE);
		coalesce_op->children.push_back(std::move(children[0]));
		coalesce_op->children.push_back(std::move(children[1]));
		return std::move(coalesce_op);
	}

	auto function = make_unique<FunctionExpression>(std::move(catalog), std::move(schema), lowercase_name.c_str(),
	                                                std::move(children), std::move(filter_expr), std::move(order_bys),
	                                                root->agg_distinct, false, root->export_state);
	function->query_location = root->location;

	return std::move(function);
}

static string SQLValueOpToString(duckdb_libpgquery::PGSQLValueFunctionOp op) {
	switch (op) {
	case duckdb_libpgquery::PG_SVFOP_CURRENT_DATE:
		return "current_date";
	case duckdb_libpgquery::PG_SVFOP_CURRENT_TIME:
		return "get_current_time";
	case duckdb_libpgquery::PG_SVFOP_CURRENT_TIME_N:
		return "current_time_n";
	case duckdb_libpgquery::PG_SVFOP_CURRENT_TIMESTAMP:
		return "get_current_timestamp";
	case duckdb_libpgquery::PG_SVFOP_CURRENT_TIMESTAMP_N:
		return "current_timestamp_n";
	case duckdb_libpgquery::PG_SVFOP_LOCALTIME:
		return "current_localtime";
	case duckdb_libpgquery::PG_SVFOP_LOCALTIME_N:
		return "current_localtime_n";
	case duckdb_libpgquery::PG_SVFOP_LOCALTIMESTAMP:
		return "current_localtimestamp";
	case duckdb_libpgquery::PG_SVFOP_LOCALTIMESTAMP_N:
		return "current_localtimestamp_n";
	case duckdb_libpgquery::PG_SVFOP_CURRENT_ROLE:
		return "current_role";
	case duckdb_libpgquery::PG_SVFOP_CURRENT_USER:
		return "current_user";
	case duckdb_libpgquery::PG_SVFOP_USER:
		return "user";
	case duckdb_libpgquery::PG_SVFOP_SESSION_USER:
		return "session_user";
	case duckdb_libpgquery::PG_SVFOP_CURRENT_CATALOG:
		return "current_catalog";
	case duckdb_libpgquery::PG_SVFOP_CURRENT_SCHEMA:
		return "current_schema";
	default:
		throw InternalException("Could not find named SQL value function specification " + to_string((int)op));
	}
}

unique_ptr<ParsedExpression> Transformer::TransformSQLValueFunction(duckdb_libpgquery::PGSQLValueFunction *node) {
	D_ASSERT(node);
	vector<unique_ptr<ParsedExpression>> children;
	auto fname = SQLValueOpToString(node->op);
	return make_unique<FunctionExpression>(fname, std::move(children));
}

} // namespace duckdb
