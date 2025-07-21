#include "duckdb/parser/transformer.hpp"
#include "duckdb/parser/statement/merge_into_statement.hpp"
#include "nodes/parsenodes.hpp"
#include "duckdb/common/enum_util.hpp"

namespace duckdb {

unique_ptr<MergeIntoAction> Transformer::TransformMergeIntoAction(duckdb_libpgquery::PGMatchAction &action) {
	auto result = make_uniq<MergeIntoAction>();
	if (action.andClause) {
		result->condition = TransformExpression(action.andClause);
	}
	switch (action.actionType) {
	case duckdb_libpgquery::MERGE_ACTION_TYPE_UPDATE:
		result->action_type = MergeActionType::MERGE_UPDATE;
		if (action.updateTargets) {
			result->update_info = TransformUpdateSetInfo(action.updateTargets, nullptr);
		}
		result->column_order = TransformColumnOrder(action.insert_column_order);
		break;
	case duckdb_libpgquery::MERGE_ACTION_TYPE_DELETE:
		result->action_type = MergeActionType::MERGE_DELETE;
		break;
	case duckdb_libpgquery::MERGE_ACTION_TYPE_INSERT:
		result->action_type = MergeActionType::MERGE_INSERT;
		if (action.insertCols) {
			result->insert_columns = TransformInsertColumns(*action.insertCols);
		}
		if (action.insertValues) {
			TransformExpressionList(*action.insertValues, result->expressions);
		}
		result->column_order = TransformColumnOrder(action.insert_column_order);
		result->default_values = action.defaultValues;
		break;
	case duckdb_libpgquery::MERGE_ACTION_TYPE_DO_NOTHING:
		result->action_type = MergeActionType::MERGE_DO_NOTHING;
		break;
	case duckdb_libpgquery::MERGE_ACTION_TYPE_ERROR:
		result->action_type = MergeActionType::MERGE_ERROR;
		if (action.errorMessage) {
			result->expressions.push_back(TransformExpression(*action.errorMessage));
		}
		break;
	default:
		throw InternalException("Unsupported merge into action");
	}
	return result;
}

unique_ptr<SQLStatement> Transformer::TransformMergeInto(duckdb_libpgquery::PGMergeIntoStmt &stmt) {
	auto result = make_uniq<MergeIntoStatement>();

	if (stmt.withClause) {
		auto with_clause = PGPointerCast<duckdb_libpgquery::PGWithClause>(stmt.withClause);
		TransformCTE(*with_clause, result->cte_map);
	}
	result->target = TransformRangeVar(*stmt.targetTable);
	result->source = TransformTableRefNode(*stmt.source);
	if (stmt.joinCondition) {
		result->join_condition = TransformExpression(*stmt.joinCondition);
	}
	if (stmt.usingClause) {
		result->using_columns = TransformUsingClause(*stmt.usingClause);
	}

	unique_ptr<MergeIntoAction> when_matched_action;
	unique_ptr<MergeIntoAction> when_not_matched_action;

	map<MergeActionCondition, unique_ptr<MergeIntoAction>> unconditional_actions;
	for (auto cell = stmt.matchActions->head; cell; cell = cell->next) {
		auto match_action = PGPointerCast<duckdb_libpgquery::PGMatchAction>(cell->data.ptr_value);
		auto action = TransformMergeIntoAction(*match_action);
		MergeActionCondition action_condition;
		switch (match_action->when) {
		case duckdb_libpgquery::MERGE_ACTION_WHEN_MATCHED:
			action_condition = MergeActionCondition::WHEN_MATCHED;
			break;
		case duckdb_libpgquery::MERGE_ACTION_WHEN_NOT_MATCHED_BY_SOURCE:
			action_condition = MergeActionCondition::WHEN_NOT_MATCHED_BY_SOURCE;
			break;
		case duckdb_libpgquery::MERGE_ACTION_WHEN_NOT_MATCHED_BY_TARGET:
			action_condition = MergeActionCondition::WHEN_NOT_MATCHED_BY_TARGET;
			break;
		default:
			throw InternalException("Unknown merge action");
		}
		if (!action->condition) {
			// unconditional action - check if we already have an unconditional action for this match
			auto entry = unconditional_actions.find(action_condition);
			if (entry != unconditional_actions.end()) {
				string action_condition_str = MergeIntoStatement::ActionConditionToString(action_condition);
				throw ParserException(
				    "Unconditional %s clause was already defined - only one unconditional %s clause is supported",
				    action_condition_str, action_condition_str);
			}
			unconditional_actions.emplace(action_condition, std::move(action));
			continue;
		}
		result->actions[action_condition].push_back(std::move(action));
	}
	// finally add the unconditional actions
	for (auto &entry : unconditional_actions) {
		result->actions[entry.first].push_back(std::move(entry.second));
	}
	if (stmt.returningList) {
		TransformExpressionList(*stmt.returningList, result->returning_list);
	}
	return std::move(result);
}

} // namespace duckdb
