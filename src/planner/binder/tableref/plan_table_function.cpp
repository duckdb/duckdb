#include "duckdb/planner/binder.hpp"
#include "duckdb/planner/tableref/bound_table_function.hpp"

namespace duckdb {

unique_ptr<LogicalOperator> Binder::CreatePlan(BoundTableFunction &ref) {
	if (ref.subquery) {
		auto child_node = CreatePlan(*ref.subquery);
		ref.get->children.push_back(std::move(child_node));
	}
	return std::move(ref.get);
}

} // namespace duckdb
