//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/planner/operator/logical_detach.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/planner/logical_operator.hpp"
#include "duckdb/parser/parsed_data/detach_info.hpp"

namespace duckdb {

//! LogicalDetach represents a DETACH statement
class LogicalDetach : public LogicalOperator {
public:
	static constexpr const LogicalOperatorType TYPE = LogicalOperatorType::LOGICAL_DETACH;

public:
	explicit LogicalDetach(unique_ptr<DetachInfo> info);
	~LogicalDetach() override;

	unique_ptr<DetachInfo> info;

public:
	void Serialize(Serializer &serializer) const override;
	static unique_ptr<LogicalOperator> Deserialize(Deserializer &deserializer);
	idx_t EstimateCardinality(ClientContext &context) override;

protected:
	void ResolveTypes() override;

private:
	explicit LogicalDetach(unique_ptr<ParseInfo> info);
};

} // namespace duckdb
