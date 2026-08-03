//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/planner/operator/logical_alter.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/planner/logical_operator.hpp"
#include "duckdb/parser/parsed_data/alter_info.hpp"

namespace duckdb {

//! LogicalAlter represents an ALTER statement
class LogicalAlter : public LogicalOperator {
public:
	static constexpr const LogicalOperatorType TYPE = LogicalOperatorType::LOGICAL_ALTER;

public:
	explicit LogicalAlter(unique_ptr<AlterInfo> info);
	~LogicalAlter() override;

	unique_ptr<AlterInfo> info;

public:
	void Serialize(Serializer &serializer) const override;
	static unique_ptr<LogicalOperator> Deserialize(Deserializer &deserializer);
	idx_t EstimateCardinality(ClientContext &context) override;

protected:
	void ResolveTypes() override;

private:
	explicit LogicalAlter(unique_ptr<ParseInfo> info);
};

} // namespace duckdb
