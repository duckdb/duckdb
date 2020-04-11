#include "duckdb/planner/binder.hpp"
#include "duckdb/planner/operator/logical_get.hpp"
#include "duckdb/planner/tableref/bound_basetableref.hpp"

using namespace duckdb;
using namespace std;

unique_ptr<LogicalOperator> Binder::CreatePlan(BoundBaseTableRef &ref) {
	return move(ref.get);
}
