#include "duckdb/planner/binder.hpp"
#include "duckdb/planner/tableref/bound_table_function.hpp"

namespace duckdb {

unique_ptr<LogicalOperator> Binder::CreatePlan(BoundTableFunction &ref) {
	if (ref.subquery) {
		auto child_node = CreatePlan(*ref.subquery);

		LogicalOperator *node = ref.get.get();

		while (!node->children.empty()) {
			D_ASSERT(node->children.size() == 1);
			if (node->children.size() != 1) {
				throw InternalException(
				    "Binder::CreatePlan<BoundTableFunction>: linear path expected, but found node with %d children",
				    node->children.size());
			}
			node = node->children[0].get();
		}

		D_ASSERT(node->type == LogicalOperatorType::LOGICAL_GET);
		node->children.push_back(std::move(child_node));
	}
	return std::move(ref.get);
}

} // namespace duckdb
