#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"
#include "duckdb/execution/operator/persistent/physical_merge_into.hpp"
#include "duckdb/execution/physical_plan_generator.hpp"
#include "duckdb/planner/operator/logical_merge_into.hpp"
#include "duckdb/catalog/duck_catalog.hpp"

namespace duckdb {

PhysicalOperator &DuckCatalog::PlanMergeInto(ClientContext &context, PhysicalPlanGenerator &planner,
                                             LogicalMergeInto &op, PhysicalOperator &plan) {
	// FIXME: bind the merge into clauses
	vector<unique_ptr<MergeIntoOperator>> when_matched_actions;
	vector<unique_ptr<MergeIntoOperator>> when_not_matched_actions;

	auto &result =
	    planner.Make<PhysicalMergeInto>(op.types, std::move(when_matched_actions), std::move(when_not_matched_actions));
	result.children.push_back(plan);
	return result;
}

PhysicalOperator &Catalog::PlanMergeInto(ClientContext &context, PhysicalPlanGenerator &planner, LogicalMergeInto &op,
                                         PhysicalOperator &plan) {
	throw NotImplementedException("Database does not support merge into");
}

PhysicalOperator &PhysicalPlanGenerator::CreatePlan(LogicalMergeInto &op) {
	auto &plan = CreatePlan(*op.children[0]);
	D_ASSERT(op.children.size() == 1);
	dependencies.AddDependency(op.table);
	return op.table.catalog.PlanMergeInto(context, *this, op, plan);
}

} // namespace duckdb
