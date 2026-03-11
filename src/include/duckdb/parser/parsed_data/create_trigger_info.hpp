//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/parser/parsed_data/create_trigger_info.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/parser/parsed_data/create_info.hpp"
#include "duckdb/common/enums/trigger_type.hpp"
#include "duckdb/parser/sql_statement.hpp"
#include "duckdb/parser/tableref/basetableref.hpp"

namespace duckdb {

struct CreateTriggerInfo : public CreateInfo {
	CreateTriggerInfo();

	//! Trigger name
	string trigger_name;
	//! The table the trigger is on
	unique_ptr<BaseTableRef> base_table;
	//! When the trigger fires (BEFORE/AFTER/INSTEAD OF)
	TriggerTiming timing;
	//! The event that fires the trigger (INSERT/DELETE/UPDATE)
	TriggerEventType event_type;
	//! Columns for UPDATE OF
	vector<string> columns;
	//! Whether this is FOR EACH ROW
	bool for_each_row;
	//! The SQL body text (for serialization)
	string sql_body_text;
	//! The parsed SQL body of the trigger (not serialized)
	unique_ptr<SQLStatement> sql_body;

public:
	unique_ptr<CreateInfo> Copy() const override;
	string ToString() const override;

	DUCKDB_API void Serialize(Serializer &serializer) const override;
	DUCKDB_API static unique_ptr<CreateInfo> Deserialize(Deserializer &deserializer);
};

} // namespace duckdb
