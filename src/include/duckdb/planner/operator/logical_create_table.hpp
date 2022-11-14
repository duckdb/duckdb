//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/planner/operator/logical_create_table.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/planner/parsed_data/bound_create_table_info.hpp"
#include "duckdb/planner/logical_operator.hpp"

namespace duckdb {

class LogicalCreateTable : public LogicalOperator {
public:
	LogicalCreateTable(SchemaCatalogEntry *schema, unique_ptr<BoundCreateTableInfo> info)
	    : LogicalOperator(LogicalOperatorType::LOGICAL_CREATE_TABLE), schema(schema), info(move(info)) {
	}

	//! Schema to insert to
	SchemaCatalogEntry *schema;
	//! Create Table information
	unique_ptr<BoundCreateTableInfo> info;

public:
	void Serialize(FieldWriter &writer) const override;
	static unique_ptr<LogicalOperator> Deserialize(LogicalDeserializationState &state, FieldReader &reader);
	idx_t EstimateCardinality(ClientContext &context) override;

protected:
	void ResolveTypes() override {
		types.emplace_back(LogicalType::BIGINT);
	}
};
} // namespace duckdb
