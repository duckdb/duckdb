#include "duckdb/execution/operator/set/physical_recursive_cte.hpp"
#include "duckdb/execution/operator/scan/physical_column_data_scan.hpp"
#include "duckdb/execution/physical_plan_generator.hpp"
#include "duckdb/planner/expression/bound_reference_expression.hpp"
#include "duckdb/planner/operator/logical_recursive_cte.hpp"
#include "duckdb/planner/operator/logical_cteref.hpp"
#include "duckdb/common/types/column_data_collection.hpp"

namespace duckdb {

unique_ptr<PhysicalOperator> PhysicalPlanGenerator::CreatePlan(LogicalRecursiveCTE &op) {
	D_ASSERT(op.children.size() == 2);

	// Create the working_table that the PhysicalRecursiveCTE will use for evaluation.
	auto working_table = std::make_shared<ColumnDataCollection>(context, op.types);

	// Add the ColumnDataCollection to the context of this PhysicalPlanGenerator
	recursive_cte_tables[op.table_index] = working_table;

	auto left = CreatePlan(*op.children[0]);
	auto right = CreatePlan(*op.children[1]);

	auto cte =
	    make_unique<PhysicalRecursiveCTE>(op.types, op.union_all, move(left), move(right), op.estimated_cardinality);
	cte->working_table = working_table;

	return move(cte);
}

unique_ptr<PhysicalOperator> PhysicalPlanGenerator::CreatePlan(LogicalCTERef &op) {
	D_ASSERT(op.children.empty());

	auto chunk_scan = make_unique<PhysicalColumnDataScan>(op.types, PhysicalOperatorType::RECURSIVE_CTE_SCAN,
	                                                      op.estimated_cardinality);

	// CreatePlan of a LogicalRecursiveCTE must have happened before.
	auto cte = recursive_cte_tables.find(op.cte_index);
	if (cte == recursive_cte_tables.end()) {
		throw Exception("Referenced recursive CTE does not exist.");
	}
	chunk_scan->collection = cte->second.get();
	return move(chunk_scan);
}

} // namespace duckdb
