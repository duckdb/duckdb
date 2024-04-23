#include "duckdb/planner/binder.hpp"
#include "duckdb/planner/tableref/column_data_ref.hpp"
#include "duckdb/planner/operator/logical_column_data_get.hpp"

namespace duckdb {

unique_ptr<BoundTableRef> Binder::Bind(ColumnDataRef &ref) {
	auto root = make_uniq_base<LogicalOperator, LogicalDummyScan>(GenerateTableIndex());
	// values list, first plan any subqueries in the list
	for (auto &expr_list : ref.values) {
		for (auto &expr : expr_list) {
			PlanSubqueries(expr, root);
		}
	}
	// now create a LogicalExpressionGet from the set of expressions
	// fetch the types
	vector<LogicalType> types;
	for (auto &expr : ref.values[0]) {
		types.push_back(expr->return_type);
	}
	auto expr_get = make_uniq<LogicalExpressionGet>(ref.bind_index, types, std::move(ref.values));
	expr_get->AddChild(std::move(root));
	return std::move(expr_get);
}

} // namespace duckdb
