#include "duckdb/common/types/column/column_data_collection.hpp"
#include "duckdb/execution/operator/scan/physical_column_data_scan.hpp"
#include "duckdb/execution/operator/set/physical_cte.hpp"
#include "duckdb/execution/physical_plan_generator.hpp"
#include "duckdb/planner/expression/bound_reference_expression.hpp"
#include "duckdb/planner/operator/logical_cteref.hpp"
#include "duckdb/planner/operator/logical_materialized_cte.hpp"

namespace duckdb {

unique_ptr<PhysicalOperator> PhysicalPlanGenerator::CreatePlan(LogicalMaterializedCTE &op) {
	D_ASSERT(op.children.size() == 2);

	// Create the working_table that the PhysicalCTE will use for evaluation.
	auto working_table = make_shared_ptr<ColumnDataCollection>(context, op.children[0]->types);

	// Add the ColumnDataCollection to the context of this PhysicalPlanGenerator
	recursive_cte_tables[op.table_index] = working_table;
	materialized_ctes[op.table_index] = vector<const_reference<PhysicalOperator>>();

	// Create the plan for the left side. This is the materialization.
	auto left = CreatePlan(*op.children[0]);
	// Initialize an empty vector to collect the scan operators.
	auto right = CreatePlan(*op.children[1]);

	unique_ptr<PhysicalCTE> cte;
	cte = make_uniq<PhysicalCTE>(op.ctename, op.table_index, right->types, std::move(left), std::move(right),
	                             op.estimated_cardinality);
	cte->working_table = working_table;
	cte->cte_scans = materialized_ctes[op.table_index];

	return std::move(cte);
}

} // namespace duckdb
