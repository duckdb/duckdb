//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/planner/operator/logical_drop.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/planner/logical_operator.hpp"
#include "duckdb/parser/parsed_data/drop_info.hpp"

namespace duckdb {

//! LogicalDrop represents a DROP statement
class LogicalDrop : public LogicalOperator {
public:
	static constexpr const LogicalOperatorType TYPE = LogicalOperatorType::LOGICAL_DROP;

public:
	explicit LogicalDrop(unique_ptr<DropInfo> info);
	~LogicalDrop() override;

	unique_ptr<DropInfo> info;

public:
	void Serialize(Serializer &serializer) const override;
	static unique_ptr<LogicalOperator> Deserialize(Deserializer &deserializer);
	idx_t EstimateCardinality(ClientContext &context) override;

protected:
	void ResolveTypes() override;

private:
	explicit LogicalDrop(unique_ptr<ParseInfo> info);
};

} // namespace duckdb
