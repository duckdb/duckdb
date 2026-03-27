#include "duckdb/catalog/catalog_entry/trigger_catalog_entry.hpp"

#include "duckdb/catalog/catalog.hpp"
#include "duckdb/catalog/catalog_entry/schema_catalog_entry.hpp"
#include "duckdb/common/enum_util.hpp"
#include "duckdb/parser/keyword_helper.hpp"
#include "duckdb/parser/parsed_data/parse_info.hpp"

namespace duckdb {

TriggerCatalogEntry::TriggerCatalogEntry(Catalog &catalog, SchemaCatalogEntry &schema, CreateTriggerInfo &info)
    : StandardEntry(CatalogType::TRIGGER_ENTRY, schema, catalog, info.trigger_name),
      base_table(unique_ptr_cast<TableRef, BaseTableRef>(info.base_table->Copy())), timing(info.timing),
      event_type(info.event_type), columns(info.columns), for_each(info.for_each),
      trigger_action(info.trigger_action->Copy()) {
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
	result->for_each = for_each;
	result->trigger_action = trigger_action->Copy();
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
	ss << EnumUtil::ToString(timing);
	ss << " ";
	ss << EnumUtil::ToString(event_type);
	if (event_type == TriggerEventType::UPDATE_EVENT && !columns.empty()) {
		ss << " OF ";
		for (idx_t i = 0; i < columns.size(); i++) {
			if (i > 0) {
				ss << ", ";
			}
			ss << KeywordHelper::WriteOptionallyQuoted(columns[i]);
		}
	}
	ss << " ON ";
	ss << ParseInfo::QualifierToString(base_table->catalog_name, base_table->schema_name, base_table->table_name);
	ss << " FOR EACH " << EnumUtil::ToString(for_each);
	ss << " " << trigger_action->ToString();
	ss << ";";
	return ss.str();
}

} // namespace duckdb
