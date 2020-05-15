#include "duckdb/planner/binder.hpp"
#include "duckdb/planner/operator/logical_table_function.hpp"
#include "duckdb/planner/tableref/bound_table_function.hpp"

using namespace duckdb;
using namespace std;

unique_ptr<LogicalOperator> Binder::CreatePlan(BoundTableFunction &ref) {

	vector<SQLType> return_types;
	vector<string> names;

	if (ref.function->supports_projection) {
		auto &column_ids = ((TableFunctionData &)*ref.bind_data).column_ids;
		for (auto &col_idx : column_ids) {
			return_types.push_back(ref.return_types[col_idx]);
			names.push_back(ref.names[col_idx]);
		}
	} else {
		return_types = ref.return_types;
		names = ref.names;
	}

	return make_unique<LogicalTableFunction>(ref.function, ref.bind_index, move(ref.bind_data), move(ref.parameters),
	                                         return_types, names);
}
