#include "transformer/peg_transformer.hpp"
#include "duckdb/parser/parsed_data/create_trigger_info.hpp"

namespace duckdb {

unique_ptr<CreateStatement> PEGTransformerFactory::TransformCreateTriggerStmt(PEGTransformer &transformer,
                                                                              optional_ptr<ParseResult> parse_result) {
	// CreateTriggerStmt <- 'TRIGGER' IfNotExists? QualifiedName TriggerTiming TriggerEvent 'ON' BaseTableName
	// ForEachRow? Statement
	auto &list_pr = parse_result->Cast<ListParseResult>();
	auto if_not_exists = list_pr.Child<OptionalParseResult>(1).HasResult();
	auto trigger_name = transformer.Transform<QualifiedName>(list_pr.Child<ListParseResult>(2));
	auto timing = transformer.Transform<TriggerTiming>(list_pr.Child<ListParseResult>(3));
	auto trigger_event = transformer.Transform<TriggerEventInfo>(list_pr.Child<ListParseResult>(4));
	// index 5 is 'ON'
	auto base_table = transformer.Transform<unique_ptr<BaseTableRef>>(list_pr.Child<ListParseResult>(6));
	bool for_each_row = list_pr.Child<OptionalParseResult>(7).HasResult();
	auto sql_body = transformer.Transform<unique_ptr<SQLStatement>>(list_pr.Child<ListParseResult>(8));

	auto result = make_uniq<CreateStatement>();
	auto info = make_uniq<CreateTriggerInfo>();
	info->on_conflict = if_not_exists ? OnCreateConflict::IGNORE_ON_CONFLICT : OnCreateConflict::ERROR_ON_CONFLICT;
	info->catalog = trigger_name.catalog;
	info->schema = trigger_name.schema;
	info->trigger_name = trigger_name.name;
	info->timing = timing;
	info->event_type = trigger_event.event_type;
	info->columns = std::move(trigger_event.columns);
	info->base_table = std::move(base_table);
	info->for_each_row = for_each_row;
	info->sql_body_text = sql_body->ToString();
	info->sql_body = std::move(sql_body);
	result->info = std::move(info);
	return result;
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

} // namespace duckdb
