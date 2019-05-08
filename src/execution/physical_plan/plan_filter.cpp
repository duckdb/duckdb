#include "execution/operator/filter/physical_filter.hpp"
#include "execution/operator/scan/physical_index_scan.hpp"
#include "execution/order_index.hpp"
#include "execution/physical_plan_generator.hpp"
#include "optimizer/matcher/expression_matcher.hpp"
#include "planner/expression/bound_comparison_expression.hpp"
#include "planner/expression/bound_constant_expression.hpp"
#include "planner/operator/logical_filter.hpp"
#include "planner/operator/logical_get.hpp"
#include "storage/data_table.hpp"

using namespace duckdb;
using namespace std;

//! Attempt to create an index scan from a filter + get, if possible
// FIXME: this should not be done here
static unique_ptr<PhysicalOperator> CreateIndexScan(LogicalFilter &filter, LogicalGet &scan) {
	if (!scan.table) {
		return nullptr;
	}

	auto &storage = *scan.table->storage;

	if (storage.indexes.size() == 0) {
		// no indexes on the table, can't rewrite
		return nullptr;
	}
	// check all the indexes
	for (uint64_t j = 0; j < storage.indexes.size(); j++) {
		Value low_value, high_value, equal_value;
		int32_t low_index = -1, high_index = -1, equal_index = -1;
		auto &index = storage.indexes[j];
		// FIXME: assume every index is order index currently
		assert(index->type == IndexType::ORDER_INDEX);
		auto order_index = (OrderIndex *)index.get();
		// try to find a matching index for any of the filter expressions
		auto expr = filter.expressions[0].get();
		auto low_comparison_type = expr->type;
		auto high_comparison_type = expr->type;
		for (uint64_t i = 0; i < filter.expressions.size(); i++) {
			assert(i <= numeric_limits<int32_t>::max());

			expr = filter.expressions[i].get();
			// create a matcher for a comparison with a constant
			ComparisonExpressionMatcher matcher;
			// match on a comparison type
			matcher.expr_type = make_unique<ComparisonExpressionTypeMatcher>();
			// match on a constant comparison with the indexed expression
			matcher.matchers.push_back(make_unique<ExpressionEqualityMatcher>(order_index->expressions[0].get()));
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
				assert(Expression::Equals(bindings[1], order_index->expressions[0].get()));
				assert(bindings[2]->type == ExpressionType::VALUE_CONSTANT);

				auto constant_value = ((BoundConstantExpression *)bindings[2])->value;
				auto comparison_type = comparison->type;
				if (comparison->right.get() == bindings[1]) {
					// the expression is on the right side, we flip them around
					comparison_type = FlipComparisionExpression(comparison_type);
				}
				if (comparison_type == ExpressionType::COMPARE_EQUAL) {
					// equality value
					// equality overrides any other bounds so we just break here
					equal_index = (int32_t)i;
					equal_value = constant_value;
					break;
				} else if (comparison_type == ExpressionType::COMPARE_GREATERTHANOREQUALTO ||
				           comparison_type == ExpressionType::COMPARE_GREATERTHAN) {
					// greater than means this is a lower bound
					low_index = (int32_t)i;
					low_value = constant_value;
					low_comparison_type = comparison_type;
				} else {
					// smaller than means this is an upper bound
					high_index = (int32_t)i;
					high_value = constant_value;
					high_comparison_type = comparison_type;
				}
			}
		}
		if (equal_index >= 0 || low_index >= 0 || high_index >= 0) {
			auto index_scan =
			    make_unique<PhysicalIndexScan>(scan, *scan.table, *scan.table->storage, *order_index, scan.column_ids);
			if (equal_index >= 0) {
				index_scan->equal_value = equal_value;
				index_scan->equal_index = true;
				filter.expressions.erase(filter.expressions.begin() + equal_index);
			}
			if (low_index >= 0) {
				index_scan->low_value = low_value;
				index_scan->low_index = true;
				index_scan->low_expression_type = low_comparison_type;
				filter.expressions.erase(filter.expressions.begin() + low_index);
			}
			if (high_index >= 0) {
				index_scan->high_value = high_value;
				index_scan->high_index = true;
				index_scan->high_expression_type = high_comparison_type;
				filter.expressions.erase(filter.expressions.begin() + high_index);
			}
			return move(index_scan);
		}
	}
	return nullptr;
}

unique_ptr<PhysicalOperator> PhysicalPlanGenerator::CreatePlan(LogicalFilter &op) {
	assert(op.children.size() == 1);
	unique_ptr<PhysicalOperator> plan;
	if (op.children[0]->type == LogicalOperatorType::GET) {
		// filter + get
		// check if we can transform this into an index scan
		auto scan = (LogicalGet *)op.children[0].get();
		auto node = CreateIndexScan(op, *scan);
		if (node) {
			// we can do an index scan, move it to the root node
			plan = move(node);
		}
	}
	if (!plan) {
		plan = CreatePlan(*op.children[0]);
	}

	if (op.expressions.size() > 0) {
		// create a filter if there is anything to filter
		auto filter = make_unique<PhysicalFilter>(op, move(op.expressions));
		filter->children.push_back(move(plan));
		plan = move(filter);
	}
	return plan;
}
