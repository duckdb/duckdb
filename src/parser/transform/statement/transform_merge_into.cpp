#include "duckdb/parser/transformer.hpp"
#include "duckdb/parser/statement/merge_into_statement.hpp"
#include "nodes/parsenodes.hpp"

namespace duckdb {

unique_ptr<MergeIntoAction> Transformer::TransformMergeIntoAction(duckdb_libpgquery::PGMatchAction &action) {
	auto result = make_uniq<MergeIntoAction>();
	if (action.andClause) {
		result->condition = TransformExpression(action.andClause);
	}
	switch (action.actionType) {
	case duckdb_libpgquery::MERGE_ACTION_TYPE_UPDATE:
		result->action_type = MergeActionType::MERGE_UPDATE;
		result->update_info = TransformUpdateSetInfo(action.updateTargets, nullptr);
		break;
	case duckdb_libpgquery::MERGE_ACTION_TYPE_DELETE:
		result->action_type = MergeActionType::MERGE_DELETE;
		break;
	case duckdb_libpgquery::MERGE_ACTION_TYPE_INSERT:
		result->action_type = MergeActionType::MERGE_INSERT;
		if (action.insertCols) {
			result->insert_columns = TransformInsertColumns(*action.insertCols);
		}
		TransformExpressionList(*action.insertValues, result->expressions);
		break;
	case duckdb_libpgquery::MERGE_ACTION_TYPE_DO_NOTHING:
		result->action_type = MergeActionType::MERGE_DO_NOTHING;
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
	result->join_condition = TransformExpression(*stmt.joinCondition);

	for (auto cell = stmt.matchActions->head; cell; cell = cell->next) {
		auto match_action = PGPointerCast<duckdb_libpgquery::PGMatchAction>(cell->data.ptr_value);
		auto action = TransformMergeIntoAction(*match_action);
		switch (match_action->when) {
		case duckdb_libpgquery::MERGE_ACTION_WHEN_MATCHED:
			result->when_matched_actions.push_back(std::move(action));
			break;
		case duckdb_libpgquery::MERGE_ACTION_WHEN_NOT_MATCHED:
			result->when_not_matched_actions.push_back(std::move(action));
			break;
		default:
			throw InternalException("Unknown merge action");
		}
	}
	return std::move(result);
}

} // namespace duckdb
