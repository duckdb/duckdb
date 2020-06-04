#include "duckdb/planner/binder.hpp"
#include "duckdb/planner/operator/logical_table_function.hpp"
#include "duckdb/planner/tableref/bound_table_function.hpp"

using namespace duckdb;
using namespace std;

unique_ptr<LogicalOperator> Binder::CreatePlan(BoundTableFunction &ref) {

	auto logical_fun = make_unique<LogicalTableFunction>(ref.function, ref.bind_index, move(ref.bind_data),
	                                                     move(ref.parameters), ref.return_types, ref.names);
	for (idx_t i = 0; i < ref.return_types.size(); i++) {
		logical_fun->column_ids.push_back(i);
	}
	return move(logical_fun);
}
