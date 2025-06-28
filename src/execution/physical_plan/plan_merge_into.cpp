#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"
#include "duckdb/execution/operator/persistent/physical_merge_into.hpp"
#include "duckdb/execution/operator/persistent/physical_delete.hpp"
#include "duckdb/execution/operator/persistent/physical_insert.hpp"
#include "duckdb/execution/operator/persistent/physical_update.hpp"
#include "duckdb/execution/physical_plan_generator.hpp"
#include "duckdb/planner/operator/logical_merge_into.hpp"
#include "duckdb/catalog/duck_catalog.hpp"

namespace duckdb {

unique_ptr<MergeIntoOperator> PlanMergeIntoAction(ClientContext &context, LogicalMergeInto &op,
                                                  PhysicalPlanGenerator &planner, BoundMergeIntoAction &action) {
	auto result = make_uniq<MergeIntoOperator>();

	// FIXME: we cannot move op.bound_defaults/op.bound_constraints - need to copy
	if (action.condition) {
		throw InternalException("FIXME: support condition");
	}
	if (!op.bound_constraints.empty()) {
		throw InternalException("Move bound constraints");
	}

	result->action_type = action.action_type;
	result->condition = std::move(action.condition);
	switch (action.action_type) {
	case MergeActionType::MERGE_UPDATE: {
		result->op = planner.Make<PhysicalUpdate>(op.types, op.table, op.table.GetStorage(), std::move(action.columns),
		                                          std::move(action.expressions), std::move(op.bound_defaults),
		                                          std::move(op.bound_constraints), 1ULL, false);
		auto &cast_update = result->op->Cast<PhysicalUpdate>();
		cast_update.update_is_del_and_insert = false;
		break;
	}
	case MergeActionType::MERGE_DELETE: {
		result->op = planner.Make<PhysicalDelete>(op.types, op.table, op.table.GetStorage(),
		                                          std::move(op.bound_constraints), op.row_id_start, 1ULL, false);
		break;
	}
	case MergeActionType::MERGE_INSERT: {
		vector<unique_ptr<Expression>> set_expressions;
		vector<PhysicalIndex> set_columns;
		vector<LogicalType> set_types;
		unordered_set<column_t> on_conflict_filter;
		vector<column_t> columns_to_fetch;
		result->op = planner.Make<PhysicalInsert>(
		    op.types, op.table, std::move(op.bound_constraints), std::move(set_expressions), std::move(set_columns),
		    std::move(set_types), 1ULL, false, true, OnConflictAction::THROW, nullptr, nullptr,
		    std::move(on_conflict_filter), std::move(columns_to_fetch), false);
		result->expressions = std::move(action.expressions);
		break;
	}
	case MergeActionType::MERGE_DO_NOTHING:
	case MergeActionType::MERGE_ABORT:
		break;
	default:
		throw InternalException("Unsupported merge action");
	}
	return result;
}

PhysicalOperator &DuckCatalog::PlanMergeInto(ClientContext &context, PhysicalPlanGenerator &planner,
                                             LogicalMergeInto &op, PhysicalOperator &plan) {
	vector<unique_ptr<MergeIntoOperator>> when_matched_actions;
	vector<unique_ptr<MergeIntoOperator>> when_not_matched_actions;

	// plan the merge into clauses
	for (auto &action : op.when_matched_actions) {
		when_matched_actions.push_back(PlanMergeIntoAction(context, op, planner, *action));
	}
	for (auto &action : op.when_not_matched_actions) {
		when_not_matched_actions.push_back(PlanMergeIntoAction(context, op, planner, *action));
	}

	auto &result =
	    planner.Make<PhysicalMergeInto>(op.types, std::move(when_matched_actions), std::move(when_not_matched_actions), op.row_id_start);
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
