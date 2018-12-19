#include "parser/expression/aggregate_expression.hpp"
#include "parser/expression/cast_expression.hpp"
#include "parser/expression/function_expression.hpp"
#include "parser/expression/operator_expression.hpp"
#include "parser/expression/star_expression.hpp"
#include "parser/expression/window_expression.hpp"
#include "parser/transformer.hpp"

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
	} else if (fun_name == "rank") {
		return ExpressionType::WINDOW_RANK;
	} else if (fun_name == "rank_dense" || fun_name == "dense_rank") {
		return ExpressionType::WINDOW_RANK_DENSE;
	} else if (fun_name == "row_number") {
		return ExpressionType::WINDOW_ROW_NUMBER;
	} else if (fun_name == "first_value" || fun_name == "first") {
		return ExpressionType::WINDOW_FIRST_VALUE;
	} else if (fun_name == "last_value" || fun_name == "last") {
		return ExpressionType::WINDOW_LAST_VALUE;
	}

	return ExpressionType::INVALID;
}

unique_ptr<Expression> Transformer::TransformFuncCall(FuncCall *root) {
	auto name = root->funcname;
	string schema, function_name;
	if (name->length == 2) {
		// schema + name
		schema = reinterpret_cast<value *>(name->head->data.ptr_value)->val.str;
		function_name = reinterpret_cast<value *>(name->head->next->data.ptr_value)->val.str;
	} else {
		// unqualified name
		schema = DEFAULT_SCHEMA;
		function_name = reinterpret_cast<value *>(name->head->data.ptr_value)->val.str;
	}

	auto lowercase_name = StringUtil::Lower(function_name);

	if (root->over) {
		auto window_spec = reinterpret_cast<WindowDef *>(root->over);
		if (window_spec->refname) {
			// FIXME: implement named window specs, not now
			throw NotImplementedException("Named Windows");
		}
		if (window_spec->frameOptions != FRAMEOPTION_DEFAULTS) {
			throw NotImplementedException("Non-default window spec");
		}

		auto win_fun_type = WindowToExpressionType(lowercase_name);
		if (win_fun_type == ExpressionType::INVALID) {
			throw Exception("Unknown/unsupported window function");
		}

		unique_ptr<Expression> child = nullptr;
		if (root->args) {
			child = TransformExpression((Node *)root->args->head->data.ptr_value);
			if (!child) {
				throw Exception("Failed to transform window argument");
			}
		}
		auto expr = make_unique<WindowExpression>(win_fun_type, child ? move(child) : nullptr);

		// next: partitioning/ordering expressions
		TransformExpressionList(window_spec->partitionClause, expr->partitions);
		TransformOrderBy(window_spec->orderClause, expr->ordering);

		// finally: specifics of bounds
		// FIXME: actually interpret those
		auto bound_start = TransformExpression(window_spec->startOffset);
		auto bound_end = TransformExpression(window_spec->endOffset);
		// FIXME: interpret frameOptions
		return expr;
	}

	auto agg_fun_type = AggregateToExpressionType(lowercase_name);
	if (agg_fun_type == ExpressionType::INVALID) {
		// Normal functions (i.e. built-in functions or UDFs)
		vector<unique_ptr<Expression>> children;
		if (root->args != nullptr) {
			for (auto node = root->args->head; node != nullptr; node = node->next) {
				auto child_expr = TransformExpression((Node *)node->data.ptr_value);
				children.push_back(move(child_expr));
			}
		}
		return make_unique<FunctionExpression>(schema, lowercase_name.c_str(), children);
	} else {
		// Aggregate function

		if (root->over) {
			throw NotImplementedException("Window functions (OVER/PARTITION BY)");
		}
		if (root->agg_star) {
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
				if (agg_fun_type == ExpressionType::AGGREGATE_AVG) {
					// rewrite AVG(a) to SUM(a) / COUNT(a)
					// first create the SUM
					auto sum = make_unique<AggregateExpression>(
					    root->agg_distinct ? ExpressionType::AGGREGATE_SUM_DISTINCT : ExpressionType::AGGREGATE_SUM,
					    TransformExpression((Node *)root->args->head->data.ptr_value));
					// now create the count
					auto count = make_unique<AggregateExpression>(
					    root->agg_distinct ? ExpressionType::AGGREGATE_COUNT_DISTINCT : ExpressionType::AGGREGATE_COUNT,
					    TransformExpression((Node *)root->args->head->data.ptr_value));
					// cast both to decimal
					auto sum_cast = make_unique<CastExpression>(TypeId::DECIMAL, move(sum));
					auto count_cast = make_unique<CastExpression>(TypeId::DECIMAL, move(count));
					// create the divide operator
					return make_unique<OperatorExpression>(ExpressionType::OPERATOR_DIVIDE, TypeId::DECIMAL,
					                                       move(sum_cast), move(count_cast));
				} else {
					auto child = TransformExpression((Node *)root->args->head->data.ptr_value);
					return make_unique<AggregateExpression>(agg_fun_type, move(child));
				}
			} else {
				throw NotImplementedException("Aggregation over multiple columns not supported yet...\n");
			}
		}
	}
}
