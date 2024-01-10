//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/planner/operator/logical_show.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/planner/logical_operator.hpp"

namespace duckdb {

class LogicalShow : public LogicalOperator {
public:
	static constexpr const LogicalOperatorType TYPE = LogicalOperatorType::LOGICAL_SHOW;

public:
	LogicalShow();

	vector<LogicalType> types_select;
	vector<string> aliases;
	idx_t table_index;

public:
	void Serialize(Serializer &serializer) const override;
	static unique_ptr<LogicalOperator> Deserialize(Deserializer &deserializer);

protected:
	vector<idx_t> GetTableIndex() const override;
	void ResolveTypes() override;
	vector<ColumnBinding> GetColumnBindings() override;
};
} // namespace duckdb
