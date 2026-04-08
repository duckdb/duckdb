//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/parser/parsed_data/create_trigger_info.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include <string>

#include "duckdb/parser/parsed_data/create_info.hpp"
#include "duckdb/common/enums/trigger_type.hpp"
#include "duckdb/parser/query_node.hpp"
#include "duckdb/parser/tableref/basetableref.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/unique_ptr.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/winapi.hpp"

namespace duckdb {
class Deserializer;
class Serializer;
enum class TriggerEventType : uint8_t;

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
	//! Whether this fires FOR EACH ROW or FOR EACH STATEMENT
	TriggerForEach for_each;
	//! The trigger action (INSERT/UPDATE/DELETE as QueryNode)
	unique_ptr<QueryNode> trigger_action;

public:
	unique_ptr<CreateInfo> Copy() const override;
	string ToString() const override;

	DUCKDB_API void Serialize(Serializer &serializer) const override;
	DUCKDB_API static unique_ptr<CreateInfo> Deserialize(Deserializer &deserializer);
};

} // namespace duckdb
