#include "duckdb/planner/binder.hpp"
#include "duckdb/planner/operator/logical_get.hpp"
#include "duckdb/planner/tableref/bound_basetableref.hpp"
#include "duckdb/planner/operator/logical_delim_get.hpp"
namespace duckdb {

unique_ptr<LogicalOperator> Binder::CreatePlan(BoundDelimGetRef &ref) {
	return make_uniq<LogicalDelimGet>(ref.bind_index, ref.column_types);
}

} // namespace duckdb
