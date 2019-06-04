#include "optimizer/index_scan.hpp"

#include "optimizer/matcher/expression_matcher.hpp"
#include "parser/expression/comparison_expression.hpp"
#include "planner/expression/bound_comparison_expression.hpp"
#include "planner/expression/bound_constant_expression.hpp"
#include "planner/operator/logical_filter.hpp"
#include "planner/operator/logical_get.hpp"
#include "planner/operator/logical_index_scan.hpp"
#include "storage/data_table.hpp"
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

unique_ptr<LogicalOperator> IndexScan::TransformFilterToIndexScan(unique_ptr<LogicalOperator> op) {
	assert(op->type == LogicalOperatorType::FILTER);
	auto &filter = (LogicalFilter &)*op;
	auto get = (LogicalGet *)op->children[0].get();

	if (!get->table) {
		return op;
	}

	auto &storage = *get->table->storage;

	if (storage.indexes.size() == 0) {
		// no indexes on the table, can't rewrite
		return op;
	}

	// check all the indexes
	for (size_t j = 0; j < storage.indexes.size(); j++) {
		Value low_value, high_value, equal_value;
		index_t low_index = INVALID_INDEX, high_index = INVALID_INDEX, equal_index = INVALID_INDEX;
		auto &index = storage.indexes[j];
		// try to find a matching index for any of the filter expressions
		auto expr = filter.expressions[0].get();
		auto low_comparison_type = expr->type;
		auto high_comparison_type = expr->type;
		for (index_t i = 0; i < filter.expressions.size(); i++) {
			expr = filter.expressions[i].get();
			// create a matcher for a comparison with a constant
			ComparisonExpressionMatcher matcher;
			// match on a comparison type
			matcher.expr_type = make_unique<ComparisonExpressionTypeMatcher>();
			// match on a constant comparison with the indexed expression
			matcher.matchers.push_back(make_unique<ExpressionEqualityMatcher>(index->unbound_expressions[0].get()));
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
				if (comparison->right.get() == index->expressions[0].get()) {
					// the expression is on the right side, we flip them around
					comparison_type = FlipComparisionExpression(comparison_type);
				}
				if (comparison_type == ExpressionType::COMPARE_EQUAL) {
					// equality value
					// equality overrides any other bounds so we just break here
					equal_index = i;
					equal_value = constant_value;
					break;
				} else if (comparison_type == ExpressionType::COMPARE_GREATERTHANOREQUALTO ||
				           comparison_type == ExpressionType::COMPARE_GREATERTHAN) {
					// greater than means this is a lower bound
					low_index = i;
					low_value = constant_value;
					low_comparison_type = comparison_type;
				} else {
					// smaller than means this is an upper bound
					high_index = i;
					high_value = constant_value;
					high_comparison_type = comparison_type;
				}
			}
		}
		if (equal_index != INVALID_INDEX || low_index != INVALID_INDEX || high_index != INVALID_INDEX) {
			auto logical_index_scan = make_unique<LogicalIndexScan>(*get->table, *get->table->storage, *index,
			                                                        get->column_ids, get->table_index);
			if (equal_index != INVALID_INDEX) {
				logical_index_scan->equal_value = equal_value;
				logical_index_scan->equal_index = true;
				filter.expressions.erase(filter.expressions.begin() + equal_index);
			}
			if (low_index != INVALID_INDEX) {
				logical_index_scan->low_value = low_value;
				logical_index_scan->low_index = true;
				logical_index_scan->low_expression_type = low_comparison_type;
				filter.expressions.erase(filter.expressions.begin() + low_index);
			}
			if (high_index != INVALID_INDEX) {
				logical_index_scan->high_value = high_value;
				logical_index_scan->high_index = true;
				logical_index_scan->high_expression_type = high_comparison_type;
				filter.expressions.erase(filter.expressions.begin() + high_index);
			}
			return move(logical_index_scan);
		}
	}
	return op;
}
