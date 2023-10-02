#include "duckdb/planner/binder.hpp"
#include "duckdb/planner/operator/logical_get.hpp"
#include "duckdb/planner/tableref/bound_basetableref.hpp"

namespace duckdb {

unique_ptr<LogicalOperator> Binder::CreatePlan(BoundBaseTableRef &ref) {
	return std::move(ref.get);
}

} // namespace duckdb
