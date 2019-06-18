#include "parser/expression/aggregate_expression.hpp"
#include "parser/expression/cast_expression.hpp"
#include "parser/expression/function_expression.hpp"
#include "parser/expression/operator_expression.hpp"
#include "parser/expression/star_expression.hpp"
#include "parser/expression/window_expression.hpp"
#include "parser/transformer.hpp"
#include "common/string_util.hpp"

using namespace duckdb;
using namespace postgres;
using namespace std;

static ExpressionType AggregateToExpressionType(string &fun_name) {
	if (fun_name == "count") {
		return ExpressionType::AGGREGATE_COUNT;
	} else if (fun_name == "sum") {
		return ExpressionType::AGGREGATE_SUM;
	} else if (fun_name == "min") {
		return ExpressionType::AGGREGATE_MIN;
	} else if (fun_name == "max") {
		return ExpressionType::AGGREGATE_MAX;
	} else if (fun_name == "avg") {
		return ExpressionType::AGGREGATE_AVG;
	} else if (fun_name == "first") {
		return ExpressionType::AGGREGATE_FIRST;
	} else if (fun_name == "stddev_samp") {
		return ExpressionType::AGGREGATE_STDDEV_SAMP;
	}
	return ExpressionType::INVALID;
}

static ExpressionType WindowToExpressionType(string &fun_name) {
	if (fun_name == "sum") {
		return ExpressionType::WINDOW_SUM;
	} else if (fun_name == "count") {
		return ExpressionType::WINDOW_COUNT_STAR;
	} else if (fun_name == "min") {
		return ExpressionType::WINDOW_MIN;
	} else if (fun_name == "max") {
		return ExpressionType::WINDOW_MAX;
	} else if (fun_name == "avg") {
		return ExpressionType::WINDOW_AVG;
	} else if (fun_name == "rank") {
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
	} else if (fun_name == "cume_dist") {
		return ExpressionType::WINDOW_CUME_DIST;
	} else if (fun_name == "lead") {
		return ExpressionType::WINDOW_LEAD;
	} else if (fun_name == "lag") {
		return ExpressionType::WINDOW_LAG;
	} else if (fun_name == "ntile") {
		return ExpressionType::WINDOW_NTILE;
	}

	return ExpressionType::INVALID;
}

void Transformer::TransformWindowDef(WindowDef *window_spec, WindowExpression *expr) {
	assert(window_spec);
	assert(expr);

	// next: partitioning/ordering expressions
	TransformExpressionList(window_spec->partitionClause, expr->partitions);
	TransformOrderBy(window_spec->orderClause, expr->orders);

	// finally: specifics of bounds
	expr->start_expr = TransformExpression(window_spec->startOffset);
	expr->end_expr = TransformExpression(window_spec->endOffset);

	if ((window_spec->frameOptions & FRAMEOPTION_END_UNBOUNDED_PRECEDING) ||
	    (window_spec->frameOptions & FRAMEOPTION_START_UNBOUNDED_FOLLOWING)) {
		throw Exception(
		    "Window frames starting with unbounded following or ending in unbounded preceding make no sense");
	}

	if (window_spec->frameOptions & FRAMEOPTION_START_UNBOUNDED_PRECEDING) {
		expr->start = WindowBoundary::UNBOUNDED_PRECEDING;
	} else if (window_spec->frameOptions & FRAMEOPTION_START_UNBOUNDED_FOLLOWING) {
		expr->start = WindowBoundary::UNBOUNDED_FOLLOWING;
	} else if (window_spec->frameOptions & FRAMEOPTION_START_VALUE_PRECEDING) {
		expr->start = WindowBoundary::EXPR_PRECEDING;
	} else if (window_spec->frameOptions & FRAMEOPTION_START_VALUE_FOLLOWING) {
		expr->start = WindowBoundary::EXPR_FOLLOWING;
	} else if (window_spec->frameOptions & (FRAMEOPTION_START_CURRENT_ROW | FRAMEOPTION_RANGE)) {
		expr->start = WindowBoundary::CURRENT_ROW_RANGE;
	} else if (window_spec->frameOptions & (FRAMEOPTION_START_CURRENT_ROW | FRAMEOPTION_ROWS)) {
		expr->start = WindowBoundary::CURRENT_ROW_ROWS;
	}

	if (window_spec->frameOptions & FRAMEOPTION_END_UNBOUNDED_PRECEDING) {
		expr->end = WindowBoundary::UNBOUNDED_PRECEDING;
	} else if (window_spec->frameOptions & FRAMEOPTION_END_UNBOUNDED_FOLLOWING) {
		expr->end = WindowBoundary::UNBOUNDED_FOLLOWING;
	} else if (window_spec->frameOptions & FRAMEOPTION_END_VALUE_PRECEDING) {
		expr->end = WindowBoundary::EXPR_PRECEDING;
	} else if (window_spec->frameOptions & FRAMEOPTION_END_VALUE_FOLLOWING) {
		expr->end = WindowBoundary::EXPR_FOLLOWING;
	} else if (window_spec->frameOptions & (FRAMEOPTION_END_CURRENT_ROW | FRAMEOPTION_RANGE)) {
		expr->end = WindowBoundary::CURRENT_ROW_RANGE;
	} else if (window_spec->frameOptions & (FRAMEOPTION_END_CURRENT_ROW | FRAMEOPTION_ROWS)) {
		expr->end = WindowBoundary::CURRENT_ROW_ROWS;
	}

	assert(expr->start != WindowBoundary::INVALID && expr->end != WindowBoundary::INVALID);
	if (((expr->start == WindowBoundary::EXPR_PRECEDING || expr->start == WindowBoundary::EXPR_PRECEDING) &&
	     !expr->start_expr) ||
	    ((expr->end == WindowBoundary::EXPR_PRECEDING || expr->end == WindowBoundary::EXPR_PRECEDING) &&
	     !expr->end_expr)) {
		throw Exception("Failed to transform window boundary expression");
	}
}

unique_ptr<ParsedExpression> Transformer::TransformFuncCall(FuncCall *root) {
	auto name = root->funcname;
	string schema, function_name;
	if (name->length == 2) {
		// schema + name
		schema = reinterpret_cast<postgres::Value *>(name->head->data.ptr_value)->val.str;
		function_name = reinterpret_cast<postgres::Value *>(name->head->next->data.ptr_value)->val.str;
	} else {
		// unqualified name
		schema = DEFAULT_SCHEMA;
		function_name = reinterpret_cast<postgres::Value *>(name->head->data.ptr_value)->val.str;
	}

	auto lowercase_name = StringUtil::Lower(function_name);

	if (root->over) {

		auto win_fun_type = WindowToExpressionType(lowercase_name);
		if (win_fun_type == ExpressionType::INVALID) {
			throw Exception("Unknown/unsupported window function");
		}

		auto expr = make_unique<WindowExpression>(win_fun_type, nullptr);

		if (root->args) {
			vector<unique_ptr<ParsedExpression>> function_list;
			auto res = TransformExpressionList(root->args, function_list);
			if (!res) {
				throw Exception("Failed to transform window function children");
			}
			if (function_list.size() > 0) {
				expr->child = move(function_list[0]);
			}
			if (function_list.size() > 1) {
				assert(win_fun_type == ExpressionType::WINDOW_LEAD || win_fun_type == ExpressionType::WINDOW_LAG);
				expr->offset_expr = move(function_list[1]);
			}
			if (function_list.size() > 2) {
				assert(win_fun_type == ExpressionType::WINDOW_LEAD || win_fun_type == ExpressionType::WINDOW_LAG);
				expr->default_expr = move(function_list[2]);
			}
			assert(function_list.size() <= 3);
		}
		auto window_spec = reinterpret_cast<WindowDef *>(root->over);

		if (window_spec->name) {
			auto it = window_clauses.find(StringUtil::Lower(string(window_spec->name)));
			if (it == window_clauses.end()) {
				throw Exception("Could not find named window specification");
			}
			window_spec = it->second;
			assert(window_spec);
		}
		TransformWindowDef(window_spec, expr.get());

		return move(expr);
	}

	auto agg_fun_type = AggregateToExpressionType(lowercase_name);
	if (agg_fun_type == ExpressionType::INVALID) {
		// Normal functions (i.e. built-in functions or UDFs)
		vector<unique_ptr<ParsedExpression>> children;
		if (root->args != nullptr) {
			for (auto node = root->args->head; node != nullptr; node = node->next) {
				auto child_expr = TransformExpression((Node *)node->data.ptr_value);
				children.push_back(move(child_expr));
			}
		}
		return make_unique<FunctionExpression>(schema, lowercase_name.c_str(), children);
	} else {
		// Aggregate function
		assert(!root->over); // see above
		if (root->agg_star || (agg_fun_type == ExpressionType::AGGREGATE_COUNT && !root->args)) {
			return make_unique<AggregateExpression>(agg_fun_type, make_unique<StarExpression>());
		} else {
			if (root->agg_distinct) {
				switch (agg_fun_type) {
				case ExpressionType::AGGREGATE_COUNT:
					agg_fun_type = ExpressionType::AGGREGATE_COUNT_DISTINCT;
					break;
				case ExpressionType::AGGREGATE_SUM:
					agg_fun_type = ExpressionType::AGGREGATE_SUM_DISTINCT;
					break;
				default:
					// makes no difference for other aggregation types
					break;
				}
			}

			if (!root->args) {
				throw NotImplementedException("Aggregation over zero columns not supported!");
			} else if (root->args->length < 2) {
				auto child = TransformExpression((Node *)root->args->head->data.ptr_value);
				if (child->IsAggregate()) {
					throw ParserException("aggregate function calls cannot be nested");
				}
				if (agg_fun_type == ExpressionType::AGGREGATE_AVG) {
					// rewrite AVG(a) to SUM(a) / COUNT(a)
					// first create the SUM
					auto sum = make_unique<AggregateExpression>(
					    root->agg_distinct ? ExpressionType::AGGREGATE_SUM_DISTINCT : ExpressionType::AGGREGATE_SUM,
					    child->Copy());
					// now create the count
					auto count = make_unique<AggregateExpression>(
					    root->agg_distinct ? ExpressionType::AGGREGATE_COUNT_DISTINCT : ExpressionType::AGGREGATE_COUNT,
					    move(child));
					// cast both to decimal
					auto sum_cast = make_unique<CastExpression>(SQLType(SQLTypeId::DECIMAL), move(sum));
					auto count_cast = make_unique<CastExpression>(SQLType(SQLTypeId::DECIMAL), move(count));
					// create the divide operator
					return make_unique<OperatorExpression>(ExpressionType::OPERATOR_DIVIDE, move(sum_cast),
					                                       move(count_cast));
				} else {
					return make_unique<AggregateExpression>(agg_fun_type, move(child));
				}
			} else {
				throw NotImplementedException("Aggregation over multiple columns not supported yet...\n");
			}
		}
	}
}
