//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/planner/operator/logical_create.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/planner/logical_operator.hpp"
#include "duckdb/parser/parsed_data/create_info.hpp"
#include "duckdb/catalog/catalog_entry.hpp"
#include "duckdb/common/enums/logical_operator_type.hpp"
#include "duckdb/common/optional_ptr.hpp"
#include "duckdb/common/typedefs.hpp"
#include "duckdb/common/unique_ptr.hpp"

namespace duckdb {
class ClientContext;
class Deserializer;
class SchemaCatalogEntry;
class Serializer;

//! LogicalCreate represents a CREATE operator
class LogicalCreate : public LogicalOperator {
public:
	static constexpr const LogicalOperatorType TYPE = LogicalOperatorType::LOGICAL_INVALID;

public:
	LogicalCreate(LogicalOperatorType type, unique_ptr<CreateInfo> info,
	              optional_ptr<SchemaCatalogEntry> schema = nullptr);

	optional_ptr<SchemaCatalogEntry> schema;
	unique_ptr<CreateInfo> info;

public:
	void Serialize(Serializer &serializer) const override;
	static unique_ptr<LogicalOperator> Deserialize(Deserializer &deserializer);

	idx_t EstimateCardinality(ClientContext &context) override;

protected:
	void ResolveTypes() override;

private:
	LogicalCreate(LogicalOperatorType type, ClientContext &context, unique_ptr<CreateInfo> info);
};
} // namespace duckdb
