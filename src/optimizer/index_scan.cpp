#include "duckdb/optimizer/index_scan.hpp"
#include "duckdb/optimizer/matcher/expression_matcher.hpp"

#include "duckdb/parser/expression/comparison_expression.hpp"

#include "duckdb/planner/expression/bound_columnref_expression.hpp"
#include "duckdb/planner/expression/bound_comparison_expression.hpp"
#include "duckdb/planner/expression/bound_constant_expression.hpp"
#include "duckdb/planner/expression_iterator.hpp"
#include "duckdb/planner/operator/logical_filter.hpp"
#include "duckdb/planner/operator/logical_get.hpp"
#include "duckdb/planner/operator/logical_index_scan.hpp"

#include "duckdb/storage/data_table.hpp"
using namespace duckdb;
using namespace std;

unique_ptr<LogicalOperator> IndexScan::Optimize(unique_ptr<LogicalOperator> op) {
	if (op->type == LogicalOperatorType::FILTER && op->children[0]->type == LogicalOperatorType::GET) {
		return TransformFilterToIndexScan(move(op));
	}
	for (auto &child : op->children) {
		child = Optimize(move(child));
	}
	return op;
}

static void RewriteIndexExpression(Index &index, LogicalGet &get, Expression &expr, bool &rewrite_possible) {
	if (expr.type == ExpressionType::BOUND_COLUMN_REF) {
		auto &bound_colref = (BoundColumnRefExpression &)expr;
		// bound column ref: rewrite to fit in the current set of bound column ids
		bound_colref.binding.table_index = get.table_index;
		column_t referenced_column = index.column_ids[bound_colref.binding.column_index];
		// search for the referenced column in the set of column_ids
		for (idx_t i = 0; i < get.column_ids.size(); i++) {
			if (get.column_ids[i] == referenced_column) {
				bound_colref.binding.column_index = i;
				return;
			}
		}
		// column id not found in bound columns in the LogicalGet: rewrite not possible
		rewrite_possible = false;
	}
	ExpressionIterator::EnumerateChildren(
	    expr, [&](Expression &child) { RewriteIndexExpression(index, get, child, rewrite_possible); });
}

unique_ptr<LogicalOperator> IndexScan::TransformFilterToIndexScan(unique_ptr<LogicalOperator> op) {
	assert(op->type == LogicalOperatorType::FILTER);
	auto &filter = (LogicalFilter &)*op;
	auto get = (LogicalGet *)op->children[0].get();

	if (!get->table) {
		return op;
	}

	auto &storage = *get->table->storage;

	if (storage.info->indexes.size() == 0) {
		// no indexes on the table, can't rewrite
		return op;
	}

	// check all the indexes
	for (size_t j = 0; j < storage.info->indexes.size(); j++) {
		auto &index = storage.info->indexes[j];

		//		assert(index->unbound_expressions.size() == 1);
		// first rewrite the index expression so the ColumnBindings align with the column bindings of the current table
		if (index->unbound_expressions.size() > 1)
			continue;
		auto index_expression = index->unbound_expressions[0]->Copy();
		bool rewrite_possible = true;
		RewriteIndexExpression(*index, *get, *index_expression, rewrite_possible);
		if (!rewrite_possible) {
			// could not rewrite!
			continue;
		}

		Value low_value, high_value, equal_value;
		// try to find a matching index for any of the filter expressions
		auto expr = filter.expressions[0].get();
		auto low_comparison_type = expr->type;
		auto high_comparison_type = expr->type;
		for (idx_t i = 0; i < filter.expressions.size(); i++) {
			expr = filter.expressions[i].get();
			// create a matcher for a comparison with a constant
			ComparisonExpressionMatcher matcher;
			// match on a comparison type
			matcher.expr_type = make_unique<ComparisonExpressionTypeMatcher>();
			// match on a constant comparison with the indexed expression
			matcher.matchers.push_back(make_unique<ExpressionEqualityMatcher>(index_expression.get()));
			matcher.matchers.push_back(make_unique<ConstantExpressionMatcher>());

			matcher.policy = SetMatcher::Policy::UNORDERED;

			vector<Expression *> bindings;
			if (matcher.Match(expr, bindings)) {
				// range or equality comparison with constant value
				// we can use our index here
				// bindings[0] = the expression
				// bindings[1] = the index expression
				// bindings[2] = the constant
				auto comparison = (BoundComparisonExpression *)bindings[0];
				assert(bindings[0]->GetExpressionClass() == ExpressionClass::BOUND_COMPARISON);
				assert(bindings[2]->type == ExpressionType::VALUE_CONSTANT);

				auto constant_value = ((BoundConstantExpression *)bindings[2])->value;
				auto comparison_type = comparison->type;
				if (comparison->left->type == ExpressionType::VALUE_CONSTANT) {
					// the expression is on the right side, we flip them around
					comparison_type = FlipComparisionExpression(comparison_type);
				}
				if (comparison_type == ExpressionType::COMPARE_EQUAL) {
					// equality value
					// equality overrides any other bounds so we just break here
					equal_value = constant_value;
					break;
				} else if (comparison_type == ExpressionType::COMPARE_GREATERTHANOREQUALTO ||
				           comparison_type == ExpressionType::COMPARE_GREATERTHAN) {
					// greater than means this is a lower bound
					low_value = constant_value;
					low_comparison_type = comparison_type;
				} else {
					// smaller than means this is an upper bound
					high_value = constant_value;
					high_comparison_type = comparison_type;
				}
			}
		}
		if (!equal_value.is_null || !low_value.is_null || !high_value.is_null) {
			auto logical_index_scan = make_unique<LogicalIndexScan>(*get->table, *get->table->storage, *index,
			                                                        get->column_ids, get->table_index);
			if (!equal_value.is_null) {
				logical_index_scan->equal_value = equal_value;
				logical_index_scan->equal_index = true;
			}
			if (!low_value.is_null) {
				logical_index_scan->low_value = low_value;
				logical_index_scan->low_index = true;
				logical_index_scan->low_expression_type = low_comparison_type;
			}
			if (!high_value.is_null) {
				logical_index_scan->high_value = high_value;
				logical_index_scan->high_index = true;
				logical_index_scan->high_expression_type = high_comparison_type;
			}
			op->children[0] = move(logical_index_scan);
			break;
		}
	}
	return op;
}
