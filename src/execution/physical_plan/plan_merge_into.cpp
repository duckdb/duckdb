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

	result->action_type = action.action_type;
	result->condition = std::move(action.condition);
	vector<unique_ptr<BoundConstraint>> bound_constraints;
	for (auto &constraint : op.bound_constraints) {
		bound_constraints.push_back(constraint->Copy());
	}

	switch (action.action_type) {
	case MergeActionType::MERGE_UPDATE: {
		vector<unique_ptr<Expression>> defaults;
		for (auto &def : op.bound_defaults) {
			defaults.push_back(def->Copy());
		}
		result->op = planner.Make<PhysicalUpdate>(op.types, op.table, op.table.GetStorage(), std::move(action.columns),
		                                          std::move(action.expressions), std::move(defaults),
		                                          std::move(bound_constraints), 1ULL, false);
		auto &cast_update = result->op->Cast<PhysicalUpdate>();
		cast_update.update_is_del_and_insert = action.update_is_del_and_insert;
		break;
	}
	case MergeActionType::MERGE_DELETE: {
		result->op = planner.Make<PhysicalDelete>(op.types, op.table, op.table.GetStorage(),
		                                          std::move(bound_constraints), op.row_id_start, 1ULL, false);
		break;
	}
	case MergeActionType::MERGE_INSERT: {
		vector<unique_ptr<Expression>> set_expressions;
		vector<PhysicalIndex> set_columns;
		vector<LogicalType> set_types;
		unordered_set<column_t> on_conflict_filter;
		vector<column_t> columns_to_fetch;

		result->op = planner.Make<PhysicalInsert>(
		    op.types, op.table, std::move(bound_constraints), std::move(set_expressions), std::move(set_columns),
		    std::move(set_types), 1ULL, false, true, OnConflictAction::THROW, nullptr, nullptr,
		    std::move(on_conflict_filter), std::move(columns_to_fetch), false);
		// transform expressions if required
		if (!action.column_index_map.empty()) {
			vector<unique_ptr<Expression>> new_expressions;
			for (auto &col : op.table.GetColumns().Physical()) {
				auto storage_idx = col.StorageOid();
				auto mapped_index = action.column_index_map[col.Physical()];
				if (mapped_index == DConstants::INVALID_INDEX) {
					// push default value
					new_expressions.push_back(op.bound_defaults[storage_idx]->Copy());
				} else {
					// push reference
					new_expressions.push_back(std::move(action.expressions[mapped_index]));
				}
			}
			action.expressions = std::move(new_expressions);
		}
		result->expressions = std::move(action.expressions);
		break;
	}
	case MergeActionType::MERGE_ERROR:
		result->expressions = std::move(action.expressions);
		break;
	case MergeActionType::MERGE_DO_NOTHING:
		break;
	default:
		throw InternalException("Unsupported merge action");
	}
	return result;
}

PhysicalOperator &DuckCatalog::PlanMergeInto(ClientContext &context, PhysicalPlanGenerator &planner,
                                             LogicalMergeInto &op, PhysicalOperator &plan) {
	map<MergeActionCondition, vector<unique_ptr<MergeIntoOperator>>> actions;

	// plan the merge into clauses
	// FIXME: disable parallelism when we have multiple INSERTs because they do not work nicely together currently
	idx_t append_count = 0;
	for (auto &entry : op.actions) {
		vector<unique_ptr<MergeIntoOperator>> planned_actions;
		for (auto &action : entry.second) {
			if (action->action_type == MergeActionType::MERGE_INSERT) {
				append_count++;
			}
			if (action->action_type == MergeActionType::MERGE_UPDATE && action->update_is_del_and_insert) {
				append_count++;
			}
			planned_actions.push_back(PlanMergeIntoAction(context, op, planner, *action));
		}
		actions.emplace(entry.first, std::move(planned_actions));
	}

	bool parallel = append_count <= 1;

	auto &result =
	    planner.Make<PhysicalMergeInto>(op.types, std::move(actions), op.row_id_start, op.source_marker, parallel);
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
