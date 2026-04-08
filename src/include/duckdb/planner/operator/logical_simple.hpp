//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/planner/operator/logical_simple.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include <memory>
#include <utility>

#include "duckdb/common/enums/statement_type.hpp"
#include "duckdb/parser/parsed_data/parse_info.hpp"
#include "duckdb/planner/logical_operator.hpp"
#include "duckdb/common/enums/logical_operator_type.hpp"
#include "duckdb/common/typedefs.hpp"
#include "duckdb/common/types.hpp"
#include "duckdb/common/unique_ptr.hpp"
#include "duckdb/common/vector.hpp"

namespace duckdb {
class ClientContext;
class Deserializer;
class Serializer;

//! LogicalSimple represents a simple logical operator that only passes on the parse info
class LogicalSimple : public LogicalOperator {
public:
	static constexpr const LogicalOperatorType TYPE = LogicalOperatorType::LOGICAL_INVALID;

public:
	LogicalSimple(LogicalOperatorType type, unique_ptr<ParseInfo> info) : LogicalOperator(type), info(std::move(info)) {
	}

	unique_ptr<ParseInfo> info;

public:
	void Serialize(Serializer &serializer) const override;
	static unique_ptr<LogicalOperator> Deserialize(Deserializer &deserializer);
	idx_t EstimateCardinality(ClientContext &context) override;

protected:
	void ResolveTypes() override {
		types.emplace_back(LogicalType::BOOLEAN);
	}
};
} // namespace duckdb
