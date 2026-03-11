#include "duckdb/parser/parsed_data/create_trigger_info.hpp"
#include "duckdb/parser/keyword_helper.hpp"

namespace duckdb {

CreateTriggerInfo::CreateTriggerInfo()
    : CreateInfo(CatalogType::TRIGGER_ENTRY, INVALID_SCHEMA), timing(TriggerTiming::AFTER),
      event_type(TriggerEventType::INSERT_EVENT), for_each_row(false) {
}

unique_ptr<CreateInfo> CreateTriggerInfo::Copy() const {
	auto result = make_uniq<CreateTriggerInfo>();
	CopyProperties(*result);
	result->trigger_name = trigger_name;
	result->table_name = table_name;
	result->table_catalog = table_catalog;
	result->table_schema = table_schema;
	result->timing = timing;
	result->event_type = event_type;
	result->columns = columns;
	result->for_each_row = for_each_row;
	result->sql_body_text = sql_body_text;
	if (sql_body) {
		result->sql_body = sql_body->Copy();
	}
	return std::move(result);
}

string CreateTriggerInfo::ToString() const {
	duckdb::stringstream ss;
	ss << "CREATE";
	if (on_conflict == OnCreateConflict::REPLACE_ON_CONFLICT) {
		ss << " OR REPLACE";
	}
	ss << " TRIGGER ";
	if (on_conflict == OnCreateConflict::IGNORE_ON_CONFLICT) {
		ss << "IF NOT EXISTS ";
	}
	ss << KeywordHelper::WriteOptionallyQuoted(trigger_name);
	ss << " ";
	switch (timing) {
	case TriggerTiming::BEFORE:
		ss << "BEFORE";
		break;
	case TriggerTiming::AFTER:
		ss << "AFTER";
		break;
	case TriggerTiming::INSTEAD_OF:
		ss << "INSTEAD OF";
		break;
	}
	ss << " ";
	switch (event_type) {
	case TriggerEventType::INSERT_EVENT:
		ss << "INSERT";
		break;
	case TriggerEventType::DELETE_EVENT:
		ss << "DELETE";
		break;
	case TriggerEventType::UPDATE_EVENT:
		ss << "UPDATE";
		if (!columns.empty()) {
			ss << " OF ";
			for (idx_t i = 0; i < columns.size(); i++) {
				if (i > 0) {
					ss << ", ";
				}
				ss << KeywordHelper::WriteOptionallyQuoted(columns[i]);
			}
		}
		break;
	}
	ss << " ON ";
	ss << QualifierToString(table_catalog, table_schema, table_name);
	if (for_each_row) {
		ss << " FOR EACH ROW";
	}
	if (sql_body) {
		ss << " " << sql_body->ToString();
	}
	ss << ";";
	return ss.str();
}

} // namespace duckdb
