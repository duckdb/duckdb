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
#include "duckdb/parser/query_node.hpp"
#include "duckdb/parser/tableref/basetableref.hpp"

#include "duckdb/common/identifier.hpp"
namespace duckdb {

struct CreateTriggerInfo : public CreateInfo {
	CreateTriggerInfo();

	//! Trigger name
	const Identifier &GetTriggerName() const {
		return qualified_name.Name();
	}
	void SetTriggerName(Identifier name) {
		qualified_name = qualified_name.WithName(std::move(name));
	}
	//! The table the trigger is on
	unique_ptr<BaseTableRef> base_table;
	//! When the trigger fires (BEFORE/AFTER/INSTEAD OF)
	TriggerTiming timing;
	//! The event that fires the trigger (INSERT/DELETE/UPDATE)
	TriggerEventType event_type;
	//! Columns for UPDATE OF
	vector<Identifier> columns;
	//! Whether this fires FOR EACH ROW or FOR EACH STATEMENT
	TriggerForEach for_each;
	//! Alias for the NEW TABLE transition table (REFERENCING NEW TABLE AS <alias>)
	Identifier referencing_new_table;
	//! Alias for the OLD TABLE transition table (REFERENCING OLD TABLE AS <alias>)
	Identifier referencing_old_table;
	//! The trigger action (INSERT/UPDATE/DELETE as QueryNode)
	unique_ptr<QueryNode> trigger_action;

public:
	unique_ptr<CreateInfo> Copy() const override;
	string ToString() const override;

	DUCKDB_API void Serialize(Serializer &serializer) const override;
	DUCKDB_API static unique_ptr<CreateInfo> Deserialize(Deserializer &deserializer);
};

} // namespace duckdb
