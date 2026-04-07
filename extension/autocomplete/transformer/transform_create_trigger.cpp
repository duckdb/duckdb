#include "transformer/peg_transformer.hpp"
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

unique_ptr<CreateStatement> PEGTransformerFactory::TransformCreateTriggerStmt(PEGTransformer &transformer,
                                                                              optional_ptr<ParseResult> parse_result) {
	// CreateTriggerStmt <- 'TRIGGER' IfNotExists? TriggerName TriggerTiming TriggerEvent 'ON' BaseTableName
	// ForEachClause? TriggerBody
	auto &list_pr = parse_result->Cast<ListParseResult>();
	auto if_not_exists = list_pr.Child<OptionalParseResult>(1).HasResult();
	auto trigger_name = transformer.Transform<string>(list_pr.Child<ListParseResult>(2)); // TriggerName
	auto timing = transformer.Transform<TriggerTiming>(list_pr.Child<ListParseResult>(3));
	auto trigger_event = transformer.Transform<TriggerEventInfo>(list_pr.Child<ListParseResult>(4));
	// index 5 is 'ON'
	auto base_table = transformer.Transform<unique_ptr<BaseTableRef>>(list_pr.Child<ListParseResult>(6));
	auto &for_each_opt = list_pr.Child<OptionalParseResult>(7);
	TriggerForEach for_each = TriggerForEach::STATEMENT;
	if (for_each_opt.HasResult()) {
		for_each = transformer.Transform<TriggerForEach>(for_each_opt.optional_result);
	}
	auto trigger_action = transformer.Transform<unique_ptr<SQLStatement>>(list_pr.Child<ListParseResult>(8));

	auto result = make_uniq<CreateStatement>();
	auto info = make_uniq<CreateTriggerInfo>();
	info->on_conflict = if_not_exists ? OnCreateConflict::IGNORE_ON_CONFLICT : OnCreateConflict::ERROR_ON_CONFLICT;
	info->trigger_name = trigger_name;
	info->timing = timing;
	info->event_type = trigger_event.event_type;
	info->columns = std::move(trigger_event.columns);
	info->base_table = std::move(base_table);
	info->for_each = for_each;
	info->trigger_action = ExtractQueryNode(std::move(trigger_action));
	result->info = std::move(info);
	return result;
}

TriggerForEach PEGTransformerFactory::TransformForEachClause(PEGTransformer &transformer,
                                                             optional_ptr<ParseResult> parse_result) {
	// ForEachClause <- ForEachRow / ForEachStatement
	auto &list_pr = parse_result->Cast<ListParseResult>();
	return transformer.TransformEnum<TriggerForEach>(list_pr.Child<ChoiceParseResult>(0).result);
}

string PEGTransformerFactory::TransformTriggerName(PEGTransformer &transformer,
                                                   optional_ptr<ParseResult> parse_result) {
	auto &list_pr = parse_result->Cast<ListParseResult>();
	return list_pr.Child<IdentifierParseResult>(0).identifier;
}

TriggerTiming PEGTransformerFactory::TransformTriggerTiming(PEGTransformer &transformer,
                                                            optional_ptr<ParseResult> parse_result) {
	// TriggerTiming <- TriggerBefore / TriggerAfter / TriggerInsteadOf
	auto &list_pr = parse_result->Cast<ListParseResult>();
	return transformer.TransformEnum<TriggerTiming>(list_pr.Child<ChoiceParseResult>(0).result);
}

TriggerEventInfo PEGTransformerFactory::TransformTriggerEvent(PEGTransformer &transformer,
                                                              optional_ptr<ParseResult> parse_result) {
	// TriggerEvent <- TriggerEventUpdateOf / TriggerEventInsert / TriggerEventDelete / TriggerEventUpdate
	auto &list_pr = parse_result->Cast<ListParseResult>();
	return transformer.Transform<TriggerEventInfo>(list_pr.Child<ChoiceParseResult>(0).result);
}

TriggerEventInfo PEGTransformerFactory::TransformTriggerEventInsert(PEGTransformer &transformer,
                                                                    optional_ptr<ParseResult> parse_result) {
	TriggerEventInfo result;
	result.event_type = TriggerEventType::INSERT_EVENT;
	return result;
}

TriggerEventInfo PEGTransformerFactory::TransformTriggerEventDelete(PEGTransformer &transformer,
                                                                    optional_ptr<ParseResult> parse_result) {
	TriggerEventInfo result;
	result.event_type = TriggerEventType::DELETE_EVENT;
	return result;
}

TriggerEventInfo PEGTransformerFactory::TransformTriggerEventUpdate(PEGTransformer &transformer,
                                                                    optional_ptr<ParseResult> parse_result) {
	TriggerEventInfo result;
	result.event_type = TriggerEventType::UPDATE_EVENT;
	return result;
}

TriggerEventInfo PEGTransformerFactory::TransformTriggerEventUpdateOf(PEGTransformer &transformer,
                                                                      optional_ptr<ParseResult> parse_result) {
	// TriggerEventUpdateOf <- 'UPDATE' 'OF' TriggerColumnList
	auto &list_pr = parse_result->Cast<ListParseResult>();
	TriggerEventInfo result;
	result.event_type = TriggerEventType::UPDATE_EVENT;
	result.columns = transformer.Transform<vector<string>>(list_pr.Child<ListParseResult>(2));
	return result;
}

vector<string> PEGTransformerFactory::TransformTriggerColumnList(PEGTransformer &transformer,
                                                                 optional_ptr<ParseResult> parse_result) {
	// TriggerColumnList <- List(ColId)
	auto &list_pr = parse_result->Cast<ListParseResult>();
	auto column_list = ExtractParseResultsFromList(list_pr.Child<ListParseResult>(0));
	vector<string> result;
	for (auto &column : column_list) {
		result.push_back(transformer.Transform<string>(column));
	}
	return result;
}

unique_ptr<SQLStatement> PEGTransformerFactory::TransformTriggerBody(PEGTransformer &transformer,
                                                                     optional_ptr<ParseResult> parse_result) {
	// TriggerBody <- InsertStatement / UpdateStatement / DeleteStatement
	auto &list_pr = parse_result->Cast<ListParseResult>();
	auto &choice_pr = list_pr.Child<ChoiceParseResult>(0);
	return transformer.Transform<unique_ptr<SQLStatement>>(choice_pr.result);
}

} // namespace duckdb
