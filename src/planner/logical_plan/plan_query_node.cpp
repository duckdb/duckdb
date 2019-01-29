#include "parser/expression/bound_expression.hpp"
#include "parser/query_node.hpp"
#include "planner/logical_plan_generator.hpp"
#include "planner/operator/list.hpp"

using namespace duckdb;
using namespace std;

void LogicalPlanGenerator::VisitQueryNode(QueryNode &statement) {
	assert(root);
	if (statement.select_distinct) {
		auto node = GetProjection(root.get());
		if (!IsProjection(node->type)) {
			throw Exception("DISTINCT can only apply to projection, union or group");
		}

		auto distinct = make_unique<LogicalDistinct>();
		distinct->AddChild(move(root));
		root = move(distinct);
	}

	if (statement.HasOrder()) {
		auto order = make_unique<LogicalOrder>(move(statement.orderby));
		order->AddChild(move(root));
		root = move(order);
	}
	if (statement.HasLimit()) {
		auto limit = make_unique<LogicalLimit>(statement.limit.limit, statement.limit.offset);
		limit->AddChild(move(root));
		root = move(limit);
	}
}
