#include "parser/query_node.hpp"
#include "planner/logical_plan_generator.hpp"
#include "planner/operator/logical_distinct.hpp"
#include "planner/operator/logical_limit.hpp"
#include "planner/operator/logical_order.hpp"

using namespace duckdb;
using namespace std;

unique_ptr<LogicalOperator> LogicalPlanGenerator::VisitQueryNode(BoundQueryNode &node,
                                                                 unique_ptr<LogicalOperator> root) {
	assert(root);
	if (node.select_distinct) {
		auto distinct = make_unique<LogicalDistinct>();
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
