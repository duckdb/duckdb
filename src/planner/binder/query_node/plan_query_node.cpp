#include "duckdb/parser/query_node.hpp"
#include "duckdb/planner/binder.hpp"
#include "duckdb/planner/operator/logical_distinct.hpp"
#include "duckdb/planner/operator/logical_limit.hpp"
#include "duckdb/planner/operator/logical_order.hpp"

using namespace duckdb;
using namespace std;

unique_ptr<LogicalOperator> Binder::VisitQueryNode(BoundQueryNode &node, unique_ptr<LogicalOperator> root) {
	assert(root);
	if (node.select_distinct) {
		unique_ptr<LogicalDistinct> distinct = nullptr;
		// DISTINCT ON should give the target list
		distinct = make_unique<LogicalDistinct>(move(node.target_distincts));
		distinct->AddChild(move(root));
		root = move(distinct);
	}

	if (node.orders.size() > 0) {
		auto order = make_unique<LogicalOrder>(move(node.orders));
		order->AddChild(move(root));
		root = move(order);
	}

	if (node.limit >= 0 || node.offset >= 0) {
		auto limit = make_unique<LogicalLimit>(node.limit, node.offset);
		limit->AddChild(move(root));
		root = move(limit);
	}
	return root;
}
