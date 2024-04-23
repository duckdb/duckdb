#include "duckdb/planner/binder.hpp"
#include "duckdb/planner/tableref/bound_column_data_ref.hpp"

namespace duckdb {

unique_ptr<LogicalOperator> Binder::CreatePlan(BoundColumnDataRef &ref) {
	auto types = ref.collection.Types();
	// Create a non-owning LogicalColumnDataGet
	auto root = make_uniq_base<LogicalOperator, LogicalColumnDataGet>(ref.bind_index, std::move(types), ref.collection);
	return root;
}

} // namespace duckdb
