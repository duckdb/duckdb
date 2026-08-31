//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/planner/operator/logical_disconnect.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/planner/logical_operator.hpp"
#include "duckdb/parser/parsed_data/disconnect_info.hpp"

namespace duckdb {

//! LogicalDisconnect represents a DISCONNECT statement
class LogicalDisconnect : public LogicalOperator {
public:
	static constexpr const LogicalOperatorType TYPE = LogicalOperatorType::LOGICAL_DISCONNECT;

public:
	explicit LogicalDisconnect(unique_ptr<DisconnectInfo> info);
	~LogicalDisconnect() override;

	unique_ptr<DisconnectInfo> info;

public:
	void Serialize(Serializer &serializer) const override;
	static unique_ptr<LogicalOperator> Deserialize(Deserializer &deserializer);
	idx_t EstimateCardinality(ClientContext &context) override;

protected:
	void ResolveTypes() override;

private:
	explicit LogicalDisconnect(unique_ptr<ParseInfo> info);
};

} // namespace duckdb
