#pragma once

#include "duckdb/catalog/standard_entry.hpp"
#include "duckdb/common/enums/trigger_type.hpp"
#include "duckdb/parser/parsed_data/create_trigger_info.hpp"
#include "duckdb/parser/tableref/basetableref.hpp"

namespace duckdb {

//! A trigger catalog entry
class TriggerCatalogEntry : public StandardEntry {
public:
	static constexpr const CatalogType Type = CatalogType::TRIGGER_ENTRY;
	static constexpr const char *Name = "trigger";

public:
	TriggerCatalogEntry(Catalog &catalog, SchemaCatalogEntry &schema, CreateTriggerInfo &info);

	//! The table the trigger is on
	unique_ptr<BaseTableRef> base_table;
	//! When the trigger fires (BEFORE/AFTER/INSTEAD OF)
	TriggerTiming timing;
	//! The event that fires the trigger (INSERT/DELETE/UPDATE)
	TriggerEventType event_type;
	//! Columns for UPDATE OF
	vector<string> columns;
	//! Whether this fires FOR EACH ROW or FOR EACH STATEMENT
	TriggerForEach for_each;
	//! The parsed SQL body of the trigger (INSERT/UPDATE/DELETE as QueryNode)
	unique_ptr<QueryNode> sql_body;

public:
	unique_ptr<CatalogEntry> Copy(ClientContext &context) const override;
	unique_ptr<CreateInfo> GetInfo() const override;

	string ToSQL() const override;
};

} // namespace duckdb
