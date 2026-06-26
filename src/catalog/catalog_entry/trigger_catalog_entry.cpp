#include "duckdb/catalog/catalog_entry/trigger_catalog_entry.hpp"

#include "duckdb/catalog/catalog.hpp"
#include "duckdb/catalog/catalog_entry/schema_catalog_entry.hpp"
#include "duckdb/common/enum_util.hpp"
#include "duckdb/parser/keyword_helper.hpp"
#include "duckdb/parser/parsed_data/parse_info.hpp"

namespace duckdb {

TriggerCatalogEntry::TriggerCatalogEntry(Catalog &catalog, SchemaCatalogEntry &schema, CreateTriggerInfo &info)
    : StandardEntry(CatalogType::TRIGGER_ENTRY, schema, catalog, info.GetTriggerName()),
      base_table(unique_ptr_cast<TableRef, BaseTableRef>(info.base_table->Copy())), timing(info.timing),
      event_type(info.event_type), columns(info.columns), for_each(info.for_each),
      referencing_new_table(info.referencing_new_table), referencing_old_table(info.referencing_old_table),
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
	result->SetQualifiedName(QualifiedName(catalog.GetName(), schema.name, name));
	result->base_table = unique_ptr_cast<TableRef, BaseTableRef>(base_table->Copy());
	result->timing = timing;
	result->event_type = event_type;
	result->columns = columns;
	result->for_each = for_each;
	result->referencing_new_table = referencing_new_table;
	result->referencing_old_table = referencing_old_table;
	result->trigger_action = trigger_action->Copy();
	result->dependencies = dependencies;
	result->comment = comment;
	result->tags = tags;
	return std::move(result);
}

string TriggerCatalogEntry::ToSQL() const {
	duckdb::stringstream ss;
	ss << "CREATE TRIGGER ";
	ss << SQLIdentifier(name);
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
			ss << SQLIdentifier(columns[i]);
		}
	}
	ss << " ON ";
	ss << QualifiedName(base_table->GetQualifiedName().Catalog(), base_table->GetQualifiedName().Schema(),
	                    base_table->Table())
	          .ToString(QualifiedNameToStringMode::HIDE_DEFAULT_SCHEMA);
	if (!referencing_new_table.empty() || !referencing_old_table.empty()) {
		ss << " REFERENCING";
		if (!referencing_new_table.empty()) {
			ss << " NEW TABLE AS " << SQLIdentifier(referencing_new_table);
		}
		if (!referencing_old_table.empty()) {
			ss << " OLD TABLE AS " << SQLIdentifier(referencing_old_table);
		}
	}
	ss << " FOR EACH " << EnumUtil::ToString(for_each);
	ss << " " << trigger_action->ToString();
	ss << ";";
	return ss.str();
}

} // namespace duckdb
