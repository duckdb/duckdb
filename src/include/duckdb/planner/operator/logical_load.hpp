//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/planner/operator/logical_load.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/planner/logical_operator.hpp"
#include "duckdb/parser/parsed_data/load_info.hpp"

namespace duckdb {

//! LogicalLoad represents a LOAD/INSTALL statement
class LogicalLoad : public LogicalOperator {
public:
	static constexpr const LogicalOperatorType TYPE = LogicalOperatorType::LOGICAL_LOAD;

public:
	explicit LogicalLoad(unique_ptr<LoadInfo> info);
	~LogicalLoad() override;

	unique_ptr<LoadInfo> info;

public:
	void Serialize(Serializer &serializer) const override;
	static unique_ptr<LogicalOperator> Deserialize(Deserializer &deserializer);
	idx_t EstimateCardinality(ClientContext &context) override;

protected:
	void ResolveTypes() override;

private:
	explicit LogicalLoad(unique_ptr<ParseInfo> info);
};

} // namespace duckdb
