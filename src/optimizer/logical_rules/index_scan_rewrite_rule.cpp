
#include "optimizer/logical_rules/index_scan_rewrite_rule.hpp"

#include "planner/operator/list.hpp"
#include "storage/data_table.hpp"
#include "storage/order_index.hpp"

using namespace duckdb;
using namespace std;

IndexScanRewriteRule::IndexScanRewriteRule() {
	// we match on a filter + scan operation
	auto scan = make_unique_base<AbstractRuleNode, LogicalNodeType>(
	    LogicalOperatorType::GET);

	root = make_unique_base<AbstractRuleNode, LogicalNodeType>(
	    LogicalOperatorType::FILTER);

	root->children.push_back(move(scan));
	root->child_policy = ChildPolicy::SOME;
}

unique_ptr<LogicalOperator>
IndexScanRewriteRule::Apply(Rewriter &rewriter, LogicalOperator &op_root,
                            std::vector<AbstractOperator> &bindings,
                            bool &fixed_point) {
	auto filter = (LogicalFilter *)bindings[0].value.op;
	auto scan = (LogicalGet *)bindings[1].value.op;
	auto &storage = *scan->table->storage;

	if (storage.indexes.size() == 0) {
		// no indexes on the table, can't rewrite
		return nullptr;
	}
	if (scan->expression) {
		// already performing an index scan
		return nullptr;
	}

	for (size_t j = 0; j < storage.indexes.size(); j++) {
		auto &index = storage.indexes[j];
		// FIXME: assume every index is order index
		auto order_index = (OrderIndex *)index.get();
		// try to find a matching index for any of the filter expressions
		for (size_t i = 0; i < filter->expressions.size(); i++) {
			auto expr = filter->expressions[i].get();
			if (expr->type == ExpressionType::COMPARE_EQUAL) {
				auto comparison = (ComparisonExpression *)expr;
				if (order_index->expressions[0]->Equals(
				        comparison->children[0].get()) &&
				    comparison->children[1]->type ==
				        ExpressionType::VALUE_CONSTANT) {
					// we can use the index here!
					// push the right expression into the index scan
					scan->expression = move(comparison->children[1]);
					scan->index_id = j;
					fixed_point = false;
					filter->expressions.erase(filter->expressions.begin() + i);
					return nullptr;
				} else if (order_index->expressions[0]->Equals(
				               comparison->children[1].get()) &&
				           comparison->children[0]->type ==
				               ExpressionType::VALUE_CONSTANT) {
					// we can use the index here!
					// push the left expression into the index scan
					scan->expression = move(comparison->children[1]);
					scan->index_id = j;
					fixed_point = false;
					filter->expressions.erase(filter->expressions.begin() + i);
					return nullptr;
				}
			}
		}
	}
	// nothing found
	return nullptr;
}