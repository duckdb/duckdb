#include "duckdb/parser/peg/transformer/peg_transformer.hpp"
#include "duckdb/parser/parsed_data/create_trigger_info.hpp"
#include "duckdb/parser/statement/insert_statement.hpp"
#include "duckdb/parser/statement/update_statement.hpp"
#include "duckdb/parser/statement/delete_statement.hpp"
#include "duckdb/parser/statement/merge_into_statement.hpp"
#include "duckdb/parser/query_node/insert_query_node.hpp"
#include "duckdb/parser/query_node/update_query_node.hpp"
#include "duckdb/parser/query_node/delete_query_node.hpp"
#include "duckdb/parser/query_node/merge_query_node.hpp"

namespace duckdb {

static unique_ptr<QueryNode> ExtractQueryNode(unique_ptr<SQLStatement> stmt) {
	switch (stmt->type) {
	case StatementType::INSERT_STATEMENT:
		return unique_ptr_cast<InsertQueryNode, QueryNode>(std::move(stmt->Cast<InsertStatement>().node));
	case StatementType::UPDATE_STATEMENT:
		return unique_ptr_cast<UpdateQueryNode, QueryNode>(std::move(stmt->Cast<UpdateStatement>().node));
	case StatementType::DELETE_STATEMENT:
		return unique_ptr_cast<DeleteQueryNode, QueryNode>(std::move(stmt->Cast<DeleteStatement>().node));
	case StatementType::MERGE_INTO_STATEMENT:
		return unique_ptr_cast<MergeQueryNode, QueryNode>(std::move(stmt->Cast<MergeIntoStatement>().node));
	default:
		throw ParserException("Trigger body must be an INSERT, UPDATE, or DELETE statement");
	}
}

unique_ptr<CreateStatement> PEGTransformerFactory::TransformCreateTriggerStmt(
    PEGTransformer &transformer, const optional<bool> &if_not_exists, const Identifier &trigger_name,
    const TriggerTiming &trigger_timing, const TriggerEventInfo &trigger_event,
    unique_ptr<BaseTableRef> base_table_name, const optional<TriggerTableReferencingInfo> &referencing_clause,
    const optional<TriggerForEach> &for_each_clause, unique_ptr<SQLStatement> trigger_body) {
	auto result = make_uniq<CreateStatement>();
	auto info = make_uniq<CreateTriggerInfo>();
	info->on_conflict = if_not_exists ? OnCreateConflict::IGNORE_ON_CONFLICT : OnCreateConflict::ERROR_ON_CONFLICT;
	info->SetTriggerName(trigger_name);
	info->timing = trigger_timing;
	info->event_type = trigger_event.event_type;
	info->columns = trigger_event.columns;
	info->base_table = std::move(base_table_name);
	if (referencing_clause) {
		info->referencing_new_table = referencing_clause->new_table;
		info->referencing_old_table = referencing_clause->old_table;
	}
	if (for_each_clause) {
		info->for_each = *for_each_clause;
	}
	info->trigger_action = ExtractQueryNode(std::move(trigger_body));
	result->info = std::move(info);
	return result;
}

Identifier PEGTransformerFactory::TransformTriggerName(PEGTransformer &transformer, const Identifier &identifier) {
	return identifier;
}

TriggerTiming PEGTransformerFactory::TransformTriggerBefore(PEGTransformer &transformer) {
	return TriggerTiming::BEFORE;
}

TriggerTiming PEGTransformerFactory::TransformTriggerAfter(PEGTransformer &transformer) {
	return TriggerTiming::AFTER;
}

TriggerTiming PEGTransformerFactory::TransformTriggerInsteadOf(PEGTransformer &transformer) {
	return TriggerTiming::INSTEAD_OF;
}

TriggerEventInfo PEGTransformerFactory::TransformTriggerEventInsert(PEGTransformer &transformer) {
	TriggerEventInfo result;
	result.event_type = TriggerEventType::INSERT_EVENT;
	return result;
}

TriggerEventInfo PEGTransformerFactory::TransformTriggerEventDelete(PEGTransformer &transformer) {
	TriggerEventInfo result;
	result.event_type = TriggerEventType::DELETE_EVENT;
	return result;
}

TriggerEventInfo PEGTransformerFactory::TransformTriggerEventUpdate(PEGTransformer &transformer) {
	TriggerEventInfo result;
	result.event_type = TriggerEventType::UPDATE_EVENT;
	return result;
}

TriggerEventInfo PEGTransformerFactory::TransformTriggerEventUpdateOf(PEGTransformer &transformer,
                                                                      const vector<string> &trigger_column_list) {
	TriggerEventInfo result;
	result.event_type = TriggerEventType::UPDATE_EVENT;
	result.columns = StringsToIdentifiers(trigger_column_list);
	return result;
}

vector<string> PEGTransformerFactory::TransformTriggerColumnList(PEGTransformer &transformer,
                                                                 const vector<Identifier> &col_id) {
	return IdentifiersToStrings(col_id);
}

TriggerTableReferencingInfo PEGTransformerFactory::TransformReferencingNewTableAs(PEGTransformer &transformer,
                                                                                  const Identifier &col_id) {
	TriggerTableReferencingInfo info;
	info.new_table = Identifier(col_id);
	return info;
}

TriggerTableReferencingInfo PEGTransformerFactory::TransformReferencingOldTableAs(PEGTransformer &transformer,
                                                                                  const Identifier &col_id) {
	TriggerTableReferencingInfo info;
	info.old_table = Identifier(col_id);
	return info;
}

TriggerTableReferencingInfo
PEGTransformerFactory::TransformReferencingClause(PEGTransformer &transformer,
                                                  const TriggerTableReferencingInfo &referencing_item,
                                                  const optional<TriggerTableReferencingInfo> &referencing_item_1) {
	auto result = referencing_item;
	if (!referencing_item_1) {
		return result;
	}
	if (!referencing_item_1->new_table.empty()) {
		if (!result.new_table.empty()) {
			throw ParserException("NEW TABLE cannot be specified multiple times in REFERENCING clause");
		}
		result.new_table = referencing_item_1->new_table;
	}
	if (!referencing_item_1->old_table.empty()) {
		if (!result.old_table.empty()) {
			throw ParserException("OLD TABLE cannot be specified multiple times in REFERENCING clause");
		}
		result.old_table = referencing_item_1->old_table;
	}
	if (!result.new_table.empty() && !result.old_table.empty() && result.new_table == result.old_table) {
		throw ParserException("REFERENCING aliases must be distinct");
	}
	return result;
}

TriggerForEach PEGTransformerFactory::TransformForEachRow(PEGTransformer &transformer) {
	return TriggerForEach::ROW;
}

TriggerForEach PEGTransformerFactory::TransformForEachStatement(PEGTransformer &transformer) {
	return TriggerForEach::STATEMENT;
}

} // namespace duckdb
