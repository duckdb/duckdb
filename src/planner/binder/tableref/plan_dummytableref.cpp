#include "duckdb/planner/binder.hpp"
#include "duckdb/planner/operator/logical_dummy_scan.hpp"
#include "duckdb/planner/tableref/bound_dummytableref.hpp"

namespace duckdb {

unique_ptr<LogicalOperator> Binder::CreatePlan(BoundEmptyTableRef &ref) {
	return make_uniq<LogicalDummyScan>(ref.bind_index);
}

} // namespace duckdb
