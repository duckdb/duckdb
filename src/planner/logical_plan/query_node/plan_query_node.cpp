#include "parser/query_node.hpp"
#include "planner/logical_plan_generator.hpp"
#include "planner/operator/logical_distinct.hpp"
#include "planner/operator/logical_limit.hpp"
#include "planner/operator/logical_order.hpp"
#include "planner/operator/logical_order_limit.hpp"

using namespace duckdb;
using namespace std;

unique_ptr<LogicalOperator> LogicalPlanGenerator::VisitQueryNode(BoundQueryNode &node,
                                                                 unique_ptr<LogicalOperator> root) {
	assert(root);
	if (node.select_distinct) {
		unique_ptr<LogicalDistinct> distinct = nullptr;
		// DISTINCT ON should give the target list
		distinct = make_unique<LogicalDistinct>(move(node.target_distincts));
		distinct->AddChild(move(root));
		root = move(distinct);
	}

	if (node.orders.size() > 0 && (node.limit >= 0 || node.offset >= 0)) {
		if (node.limit == std::numeric_limits<int64_t>::max()) {
			auto order = make_unique<LogicalOrder>(move(node.orders));
			order->AddChild(move(root));
			root = move(order);

			auto limit = make_unique<LogicalLimit>(node.limit, node.offset);
			limit->AddChild(move(root));
			root = move(limit);
		} else {
			/*
			  This optimization will be applied only when ORDER BY + LIMIT are present, irrespective of OFFSET.
			  If it is an ORDER BY + OFFSET query, then this optimization doesn't apply.
			*/
			auto order_and_limit = make_unique<LogicalOrderAndLimit>(move(node.orders), node.limit, node.offset);
			order_and_limit->AddChild(move(root));
			root = move(order_and_limit);
		}
	} else if (node.orders.size() > 0) {
		auto order = make_unique<LogicalOrder>(move(node.orders));
		order->AddChild(move(root));
		root = move(order);
	} else if (node.limit >= 0 || node.offset >= 0) {
		auto limit = make_unique<LogicalLimit>(node.limit, node.offset);
		limit->AddChild(move(root));
		root = move(limit);
	}
	return root;
}
