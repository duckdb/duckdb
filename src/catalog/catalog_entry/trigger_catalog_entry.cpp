#include "duckdb/catalog/catalog_entry/trigger_catalog_entry.hpp"

#include "duckdb/catalog/catalog.hpp"
#include "duckdb/catalog/catalog_entry/schema_catalog_entry.hpp"
#include "duckdb/parser/keyword_helper.hpp"
#include "duckdb/parser/parsed_data/parse_info.hpp"

namespace duckdb {

TriggerCatalogEntry::TriggerCatalogEntry(Catalog &catalog, SchemaCatalogEntry &schema, CreateTriggerInfo &info)
    : StandardEntry(CatalogType::TRIGGER_ENTRY, schema, catalog, info.trigger_name),
      base_table(unique_ptr_cast<TableRef, BaseTableRef>(info.base_table->Copy())), timing(info.timing),
      event_type(info.event_type), columns(info.columns), for_each_row(info.for_each == TriggerForEach::ROW),
      sql_body_text(info.sql_body_text) {
	this->temporary = info.temporary;
	this->comment = info.comment;
	this->tags = info.tags;
}

unique_ptr<CatalogEntry> TriggerCatalogEntry::Copy(ClientContext &context) const {
	auto info_copy = GetInfo();
	auto &cast_info = info_copy->Cast<CreateTriggerInfo>();
	return make_uniq<TriggerCatalogEntry>(catalog, schema, cast_info);
}

unique_ptr<CreateInfo> TriggerCatalogEntry::GetInfo() const {
	auto result = make_uniq<CreateTriggerInfo>();
	result->catalog = catalog.GetName();
	result->schema = schema.name;
	result->trigger_name = name;
	result->base_table = unique_ptr_cast<TableRef, BaseTableRef>(base_table->Copy());
	result->timing = timing;
	result->event_type = event_type;
	result->columns = columns;
	result->for_each = for_each_row ? TriggerForEach::ROW : TriggerForEach::STATEMENT;
	result->sql_body_text = sql_body_text;
	result->dependencies = dependencies;
	result->comment = comment;
	result->tags = tags;
	return std::move(result);
}

string TriggerCatalogEntry::ToSQL() const {
	duckdb::stringstream ss;
	ss << "CREATE TRIGGER ";
	ss << KeywordHelper::WriteOptionallyQuoted(name);
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
	ss << ParseInfo::QualifierToString(base_table->catalog_name, base_table->schema_name, base_table->table_name);
	if (for_each_row) {
		ss << " FOR EACH ROW";
	}
	if (!sql_body_text.empty()) {
		ss << " " << sql_body_text;
	}
	ss << ";";
	return ss.str();
}

} // namespace duckdb
