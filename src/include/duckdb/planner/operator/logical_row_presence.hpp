//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/planner/operator/logical_row_presence.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/planner/logical_operator.hpp"

namespace duckdb {

//! LogicalRowPresence passes its child through unchanged and adds a single BOOLEAN column
//! (presence_index, 0) that is true for every row it emits. It is placed below the NULL-padding
//! side(s) of a join: rows fabricated by the join have the column NULL - like every other column
//! of that side - which lets row variables evaluate to NULL for fabricated rows.
class LogicalRowPresence : public LogicalOperator {
public:
	static constexpr const LogicalOperatorType TYPE = LogicalOperatorType::LOGICAL_ROW_PRESENCE;

public:
	explicit LogicalRowPresence(TableIndex presence_index);
	LogicalRowPresence(TableIndex presence_index, unique_ptr<LogicalOperator> child);

	TableIndex presence_index;

public:
	vector<ColumnBinding> GetColumnBindings() override;
	void Serialize(Serializer &serializer) const override;
	static unique_ptr<LogicalOperator> Deserialize(Deserializer &deserializer);
	vector<TableIndex> GetTableIndex() const override;

protected:
	void ResolveTypes() override;
};
} // namespace duckdb
