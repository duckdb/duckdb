//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/planner/operator/logical_attach.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/planner/logical_operator.hpp"
#include "duckdb/parser/parsed_data/attach_info.hpp"

namespace duckdb {

//! LogicalAttach represents an ATTACH statement
class LogicalAttach : public LogicalOperator {
public:
	static constexpr const LogicalOperatorType TYPE = LogicalOperatorType::LOGICAL_ATTACH;

public:
	explicit LogicalAttach(unique_ptr<AttachInfo> info);
	~LogicalAttach() override;

	unique_ptr<AttachInfo> info;

public:
	void Serialize(Serializer &serializer) const override;
	static unique_ptr<LogicalOperator> Deserialize(Deserializer &deserializer);
	idx_t EstimateCardinality(ClientContext &context) override;

protected:
	void ResolveTypes() override;

private:
	explicit LogicalAttach(unique_ptr<ParseInfo> info);
};

} // namespace duckdb
