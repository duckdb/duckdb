//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/planner/operator/logical_connect.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/planner/logical_operator.hpp"
#include "duckdb/parser/parsed_data/connect_info.hpp"

namespace duckdb {

//! LogicalConnect represents a CONNECT statement
class LogicalConnect : public LogicalOperator {
public:
	static constexpr const LogicalOperatorType TYPE = LogicalOperatorType::LOGICAL_CONNECT;

public:
	explicit LogicalConnect(unique_ptr<ConnectInfo> info);
	~LogicalConnect() override;

	unique_ptr<ConnectInfo> info;

public:
	void Serialize(Serializer &serializer) const override;
	static unique_ptr<LogicalOperator> Deserialize(Deserializer &deserializer);
	idx_t EstimateCardinality(ClientContext &context) override;

protected:
	void ResolveTypes() override;

private:
	explicit LogicalConnect(unique_ptr<ParseInfo> info);
};

} // namespace duckdb
