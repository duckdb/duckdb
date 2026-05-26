#include "duckdb/parser/peg/transformer/peg_transformer.hpp"
#include "duckdb/parser/parsed_data/create_trigger_info.hpp"
#include "duckdb/parser/statement/insert_statement.hpp"
#include "duckdb/parser/statement/update_statement.hpp"
#include "duckdb/parser/statement/delete_statement.hpp"
#include "duckdb/parser/query_node/insert_query_node.hpp"
#include "duckdb/parser/query_node/update_query_node.hpp"
#include "duckdb/parser/query_node/delete_query_node.hpp"

namespace duckdb {

static unique_ptr<QueryNode> ExtractQueryNode(unique_ptr<SQLStatement> stmt) {
	switch (stmt->type) {
	case StatementType::INSERT_STATEMENT:
		return unique_ptr_cast<InsertQueryNode, QueryNode>(std::move(stmt->Cast<InsertStatement>().node));
	case StatementType::UPDATE_STATEMENT:
		return unique_ptr_cast<UpdateQueryNode, QueryNode>(std::move(stmt->Cast<UpdateStatement>().node));
	case StatementType::DELETE_STATEMENT:
		return unique_ptr_cast<DeleteQueryNode, QueryNode>(std::move(stmt->Cast<DeleteStatement>().node));
	default:
		throw ParserException("Trigger body must be an INSERT, UPDATE, or DELETE statement");
	}
}

unique_ptr<CreateStatement> PEGTransformerFactory::TransformCreateTriggerStmt(
    PEGTransformer &transformer, const bool &if_not_exists, const string &trigger_name,
    const TriggerTiming &trigger_timing, const TriggerEventInfo &trigger_event,
    unique_ptr<BaseTableRef> base_table_name, const TriggerForEach &for_each_clause,
    unique_ptr<SQLStatement> trigger_body) {
	auto result = make_uniq<CreateStatement>();
	auto info = make_uniq<CreateTriggerInfo>();
	info->on_conflict = if_not_exists ? OnCreateConflict::IGNORE_ON_CONFLICT : OnCreateConflict::ERROR_ON_CONFLICT;
	info->trigger_name = trigger_name;
	info->timing = trigger_timing;
	info->event_type = trigger_event.event_type;
	info->columns = trigger_event.columns;
	info->base_table = std::move(base_table_name);
	info->for_each = for_each_clause;
	info->trigger_action = ExtractQueryNode(std::move(trigger_body));
	result->info = std::move(info);
	return result;
}

string PEGTransformerFactory::TransformTriggerName(PEGTransformer &transformer, const string &identifier) {
	return identifier;
}

TriggerForEach PEGTransformerFactory::TransformForEachClause(PEGTransformer &transformer, ParseResult &parse_result) {
	auto &list_pr = parse_result.Cast<ListParseResult>();
	return transformer.TransformEnum<TriggerForEach>(list_pr.Child<ChoiceParseResult>(0).GetResult());
}

TriggerTiming PEGTransformerFactory::TransformTriggerTiming(PEGTransformer &transformer, ParseResult &parse_result) {
	auto &list_pr = parse_result.Cast<ListParseResult>();
	return transformer.TransformEnum<TriggerTiming>(list_pr.Child<ChoiceParseResult>(0).GetResult());
}

TriggerEventInfo PEGTransformerFactory::TransformTriggerEvent(PEGTransformer &transformer, ParseResult &parse_result) {
	auto &list_pr = parse_result.Cast<ListParseResult>();
	return transformer.Transform<TriggerEventInfo>(list_pr.Child<ChoiceParseResult>(0).GetResult());
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
	result.columns = trigger_column_list;
	return result;
}

vector<string> PEGTransformerFactory::TransformTriggerColumnList(PEGTransformer &transformer,
                                                                 const vector<string> &col_id) {
	return col_id;
}

TriggerForEach PEGTransformerFactory::TransformForEachRow(PEGTransformer &transformer) {
	return TriggerForEach::ROW;
}

TriggerForEach PEGTransformerFactory::TransformForEachStatement(PEGTransformer &transformer) {
	return TriggerForEach::STATEMENT;
}

} // namespace duckdb
