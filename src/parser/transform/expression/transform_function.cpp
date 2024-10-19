#include "duckdb/common/enum_util.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/common/to_string.hpp"
#include "duckdb/parser/expression/case_expression.hpp"
#include "duckdb/parser/expression/cast_expression.hpp"
#include "duckdb/parser/expression/constant_expression.hpp"
#include "duckdb/parser/expression/function_expression.hpp"

#include "duckdb/parser/expression/operator_expression.hpp"
#include "duckdb/parser/expression/star_expression.hpp"
#include "duckdb/parser/expression/window_expression.hpp"
#include "duckdb/parser/transformer.hpp"

namespace duckdb {

void Transformer::TransformWindowDef(duckdb_libpgquery::PGWindowDef &window_spec, WindowExpression &expr,
                                     const char *window_name) {
	// next: partitioning/ordering expressions
	if (window_spec.partitionClause) {
		if (window_name && !expr.partitions.empty()) {
			throw ParserException("Cannot override PARTITION BY clause of window \"%s\"", window_name);
		}
		TransformExpressionList(*window_spec.partitionClause, expr.partitions);
	}
	if (window_spec.orderClause) {
		if (window_name && !expr.orders.empty()) {
			throw ParserException("Cannot override ORDER BY clause of window \"%s\"", window_name);
		}
		TransformOrderBy(window_spec.orderClause, expr.orders);
		for (auto &order : expr.orders) {
			if (order.expression->GetExpressionType() == ExpressionType::STAR) {
				throw ParserException("Cannot ORDER BY ALL in a window expression");
			}
		}
	}
}

void Transformer::TransformWindowFrame(duckdb_libpgquery::PGWindowDef &window_spec, WindowExpression &expr) {
	// finally: specifics of bounds
	expr.start_expr = TransformExpression(window_spec.startOffset);
	expr.end_expr = TransformExpression(window_spec.endOffset);

	if ((window_spec.frameOptions & FRAMEOPTION_END_UNBOUNDED_PRECEDING) ||
	    (window_spec.frameOptions & FRAMEOPTION_START_UNBOUNDED_FOLLOWING)) {
		throw InternalException(
		    "Window frames starting with unbounded following or ending in unbounded preceding make no sense");
	}

	if (window_spec.frameOptions & FRAMEOPTION_GROUPS) {
		throw ParserException("GROUPS mode for window functions is not implemented yet");
	}
	const bool rangeMode = (window_spec.frameOptions & FRAMEOPTION_RANGE) != 0;
	if (window_spec.frameOptions & FRAMEOPTION_START_UNBOUNDED_PRECEDING) {
		expr.start = WindowBoundary::UNBOUNDED_PRECEDING;
	} else if (window_spec.frameOptions & FRAMEOPTION_START_OFFSET_PRECEDING) {
		expr.start = rangeMode ? WindowBoundary::EXPR_PRECEDING_RANGE : WindowBoundary::EXPR_PRECEDING_ROWS;
	} else if (window_spec.frameOptions & FRAMEOPTION_START_OFFSET_FOLLOWING) {
		expr.start = rangeMode ? WindowBoundary::EXPR_FOLLOWING_RANGE : WindowBoundary::EXPR_FOLLOWING_ROWS;
	} else if (window_spec.frameOptions & FRAMEOPTION_START_CURRENT_ROW) {
		expr.start = rangeMode ? WindowBoundary::CURRENT_ROW_RANGE : WindowBoundary::CURRENT_ROW_ROWS;
	}

	if (window_spec.frameOptions & FRAMEOPTION_END_UNBOUNDED_FOLLOWING) {
		expr.end = WindowBoundary::UNBOUNDED_FOLLOWING;
	} else if (window_spec.frameOptions & FRAMEOPTION_END_OFFSET_PRECEDING) {
		expr.end = rangeMode ? WindowBoundary::EXPR_PRECEDING_RANGE : WindowBoundary::EXPR_PRECEDING_ROWS;
	} else if (window_spec.frameOptions & FRAMEOPTION_END_OFFSET_FOLLOWING) {
		expr.end = rangeMode ? WindowBoundary::EXPR_FOLLOWING_RANGE : WindowBoundary::EXPR_FOLLOWING_ROWS;
	} else if (window_spec.frameOptions & FRAMEOPTION_END_CURRENT_ROW) {
		expr.end = rangeMode ? WindowBoundary::CURRENT_ROW_RANGE : WindowBoundary::CURRENT_ROW_ROWS;
	}

	D_ASSERT(expr.start != WindowBoundary::INVALID && expr.end != WindowBoundary::INVALID);
	if (((window_spec.frameOptions & (FRAMEOPTION_START_OFFSET_PRECEDING | FRAMEOPTION_START_OFFSET_FOLLOWING)) &&
	     !expr.start_expr) ||
	    ((window_spec.frameOptions & (FRAMEOPTION_END_OFFSET_PRECEDING | FRAMEOPTION_END_OFFSET_FOLLOWING)) &&
	     !expr.end_expr)) {
		throw InternalException("Failed to transform window boundary expression");
	}

	if (window_spec.frameOptions & FRAMEOPTION_EXCLUDE_CURRENT_ROW) {
		expr.exclude_clause = WindowExcludeMode::CURRENT_ROW;
	} else if (window_spec.frameOptions & FRAMEOPTION_EXCLUDE_GROUP) {
		expr.exclude_clause = WindowExcludeMode::GROUP;
	} else if (window_spec.frameOptions & FRAMEOPTION_EXCLUDE_TIES) {
		expr.exclude_clause = WindowExcludeMode::TIES;
	} else {
		expr.exclude_clause = WindowExcludeMode::NO_OTHER;
	}
}

bool Transformer::ExpressionIsEmptyStar(ParsedExpression &expr) {
	if (expr.expression_class != ExpressionClass::STAR) {
		return false;
	}
	auto &star = expr.Cast<StarExpression>();
	if (!star.columns && star.exclude_list.empty() && star.replace_list.empty()) {
		return true;
	}
	return false;
}

bool Transformer::InWindowDefinition() {
	if (in_window_definition) {
		return true;
	}
	if (parent) {
		return parent->InWindowDefinition();
	}
	return false;
}

unique_ptr<ParsedExpression> Transformer::TransformFuncCall(duckdb_libpgquery::PGFuncCall &root) {
	auto name = root.funcname;
	string catalog, schema, function_name;
	if (name->length == 3) {
		// catalog + schema + name
		catalog = PGPointerCast<duckdb_libpgquery::PGValue>(name->head->data.ptr_value)->val.str;
		schema = PGPointerCast<duckdb_libpgquery::PGValue>(name->head->next->data.ptr_value)->val.str;
		function_name = PGPointerCast<duckdb_libpgquery::PGValue>(name->head->next->next->data.ptr_value)->val.str;
	} else if (name->length == 2) {
		// schema + name
		catalog = INVALID_CATALOG;
		schema = PGPointerCast<duckdb_libpgquery::PGValue>(name->head->data.ptr_value)->val.str;
		function_name = PGPointerCast<duckdb_libpgquery::PGValue>(name->head->next->data.ptr_value)->val.str;
	} else if (name->length == 1) {
		// unqualified name
		catalog = INVALID_CATALOG;
		schema = INVALID_SCHEMA;
		function_name = PGPointerCast<duckdb_libpgquery::PGValue>(name->head->data.ptr_value)->val.str;
	} else {
		throw ParserException("TransformFuncCall - Expected 1, 2 or 3 qualifications");
	}

	//  transform children
	vector<unique_ptr<ParsedExpression>> children;
	if (root.args) {
		TransformExpressionList(*root.args, children);
	}
	if (children.size() == 1 && ExpressionIsEmptyStar(*children[0]) && !root.agg_distinct && !root.agg_order) {
		// COUNT(*) gets translated into COUNT()
		children.clear();
	}

	auto lowercase_name = StringUtil::Lower(function_name);
	if (root.over) {
		if (InWindowDefinition()) {
			throw ParserException("window functions are not allowed in window definitions");
		}

		const auto win_fun_type = WindowExpression::WindowToExpressionType(lowercase_name);
		if (win_fun_type == ExpressionType::INVALID) {
			throw InternalException("Unknown/unsupported window function");
		}

		if (win_fun_type != ExpressionType::WINDOW_AGGREGATE && root.agg_distinct) {
			throw ParserException("DISTINCT is not implemented for non-aggregate window functions!");
		}

		if (root.agg_order) {
			throw ParserException("ORDER BY is not implemented for window functions!");
		}

		if (win_fun_type != ExpressionType::WINDOW_AGGREGATE && root.agg_filter) {
			throw ParserException("FILTER is not implemented for non-aggregate window functions!");
		}
		if (root.export_state) {
			throw ParserException("EXPORT_STATE is not supported for window functions!");
		}

		if (win_fun_type == ExpressionType::WINDOW_AGGREGATE &&
		    root.agg_ignore_nulls != duckdb_libpgquery::PG_DEFAULT_NULLS) {
			throw ParserException("RESPECT/IGNORE NULLS is not supported for windowed aggregates");
		}

		auto expr = make_uniq<WindowExpression>(win_fun_type, std::move(catalog), std::move(schema), lowercase_name);
		expr->ignore_nulls = (root.agg_ignore_nulls == duckdb_libpgquery::PG_IGNORE_NULLS);
		expr->distinct = root.agg_distinct;

		if (root.agg_filter) {
			auto filter_expr = TransformExpression(root.agg_filter);
			expr->filter_expr = std::move(filter_expr);
		}

		if (win_fun_type == ExpressionType::WINDOW_AGGREGATE) {
			expr->children = std::move(children);
		} else {
			if (!children.empty()) {
				expr->children.push_back(std::move(children[0]));
			}
			if (win_fun_type == ExpressionType::WINDOW_LEAD || win_fun_type == ExpressionType::WINDOW_LAG) {
				if (children.size() > 1) {
					expr->offset_expr = std::move(children[1]);
				}
				if (children.size() > 2) {
					expr->default_expr = std::move(children[2]);
				}
				if (children.size() > 3) {
					throw ParserException("Incorrect number of parameters for function %s", lowercase_name);
				}
			} else if (win_fun_type == ExpressionType::WINDOW_NTH_VALUE) {
				if (children.size() > 1) {
					expr->children.push_back(std::move(children[1]));
				}
				if (children.size() > 2) {
					throw ParserException("Incorrect number of parameters for function %s", lowercase_name);
				}
			} else {
				if (children.size() > 1) {
					throw ParserException("Incorrect number of parameters for function %s", lowercase_name);
				}
			}
		}
		auto window_spec = PGPointerCast<duckdb_libpgquery::PGWindowDef>(root.over);
		if (window_spec->name) {
			auto it = window_clauses.find(string(window_spec->name));
			if (it == window_clauses.end()) {
				throw ParserException("window \"%s\" does not exist", window_spec->name);
			}
			window_spec = it->second;
			D_ASSERT(window_spec);
		}
		auto window_ref = window_spec;
		auto window_name = window_ref->refname;
		if (window_ref->refname) {
			auto it = window_clauses.find(string(window_spec->refname));
			if (it == window_clauses.end()) {
				throw ParserException("window \"%s\" does not exist", window_spec->refname);
			}
			window_ref = it->second;
			D_ASSERT(window_ref);
			if (window_ref->startOffset || window_ref->endOffset || window_ref->frameOptions != FRAMEOPTION_DEFAULTS) {
				throw ParserException("cannot copy window \"%s\" because it has a frame clause", window_spec->refname);
			}
		}
		in_window_definition = true;
		TransformWindowDef(*window_ref, *expr);
		if (window_ref != window_spec) {
			TransformWindowDef(*window_spec, *expr, window_name);
		}
		TransformWindowFrame(*window_spec, *expr);
		in_window_definition = false;
		SetQueryLocation(*expr, root.location);
		return std::move(expr);
	}

	if (root.agg_ignore_nulls != duckdb_libpgquery::PG_DEFAULT_NULLS) {
		throw ParserException("RESPECT/IGNORE NULLS is not supported for non-window functions");
	}

	unique_ptr<ParsedExpression> filter_expr;
	if (root.agg_filter) {
		filter_expr = TransformExpression(root.agg_filter);
	}

	auto order_bys = make_uniq<OrderModifier>();
	TransformOrderBy(root.agg_order, order_bys->orders);

	// Ordered aggregates can be either WITHIN GROUP or after the function arguments
	if (root.agg_within_group) {
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
		auto expr = make_uniq<CaseExpression>();
		CaseCheck check;
		check.when_expr = std::move(children[0]);
		check.then_expr = std::move(children[1]);
		expr->case_checks.push_back(std::move(check));
		expr->else_expr = std::move(children[2]);
		return std::move(expr);
	} else if (lowercase_name == "construct_array") {
		auto construct_array = make_uniq<OperatorExpression>(ExpressionType::ARRAY_CONSTRUCTOR);
		construct_array->children = std::move(children);
		return std::move(construct_array);
	} else if (lowercase_name == "__internal_position_operator") {
		if (children.size() != 2) {
			throw ParserException("Wrong number of arguments to __internal_position_operator.");
		}
		// swap arguments for POSITION(x IN y)
		std::swap(children[0], children[1]);
		lowercase_name = "position";
	} else if (lowercase_name == "ifnull") {
		if (children.size() != 2) {
			throw ParserException("Wrong number of arguments to IFNULL.");
		}

		//  Two-argument COALESCE
		auto coalesce_op = make_uniq<OperatorExpression>(ExpressionType::OPERATOR_COALESCE);
		coalesce_op->children.push_back(std::move(children[0]));
		coalesce_op->children.push_back(std::move(children[1]));
		return std::move(coalesce_op);
	} else if (lowercase_name == "list" && order_bys->orders.size() == 1) {
		// list(expr ORDER BY expr <sense> <nulls>) => list_sort(list(expr), <sense>, <nulls>)
		if (children.size() != 1) {
			throw ParserException("Wrong number of arguments to LIST.");
		}
		auto arg_expr = children[0].get();
		auto &order_by = order_bys->orders[0];
		if (arg_expr->Equals(*order_by.expression)) {
			auto sense = make_uniq<ConstantExpression>(EnumUtil::ToChars(order_by.type));
			auto nulls = make_uniq<ConstantExpression>(EnumUtil::ToChars(order_by.null_order));
			order_bys = nullptr;
			auto unordered = make_uniq<FunctionExpression>(catalog, schema, lowercase_name.c_str(), std::move(children),
			                                               std::move(filter_expr), std::move(order_bys),
			                                               root.agg_distinct, false, root.export_state);
			lowercase_name = "list_sort";
			order_bys.reset();   // NOLINT
			filter_expr.reset(); // NOLINT
			children.clear();    // NOLINT
			root.agg_distinct = false;
			children.emplace_back(std::move(unordered));
			children.emplace_back(std::move(sense));
			children.emplace_back(std::move(nulls));
		}
	}

	auto function = make_uniq<FunctionExpression>(std::move(catalog), std::move(schema), lowercase_name.c_str(),
	                                              std::move(children), std::move(filter_expr), std::move(order_bys),
	                                              root.agg_distinct, false, root.export_state);
	SetQueryLocation(*function, root.location);

	return std::move(function);
}

unique_ptr<ParsedExpression> Transformer::TransformSQLValueFunction(duckdb_libpgquery::PGSQLValueFunction &node) {
	throw InternalException("SQL value functions should not be emitted by the parser");
}

} // namespace duckdb
