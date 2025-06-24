#include "duckdb/planner/binder.hpp"
#include "duckdb/planner/tableref/bound_column_data_ref.hpp"
#include "duckdb/planner/operator/logical_column_data_get.hpp"

namespace duckdb {

unique_ptr<LogicalOperator> Binder::CreatePlan(BoundColumnDataRef &ref) {
	auto types = ref.collection->Types();
	// Create a (potentially owning) LogicalColumnDataGet
	auto root = make_uniq_base<LogicalOperator, LogicalColumnDataGet>(ref.bind_index, std::move(types),
	                                                                  std::move(ref.collection));
	return root;
}

} // namespace duckdb
