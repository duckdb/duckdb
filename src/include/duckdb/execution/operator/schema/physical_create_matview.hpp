#pragma once

#include "duckdb/execution/physical_operator.hpp"
#include "duckdb/parser/parsed_data/create_matview_info.hpp"

namespace duckdb {

//! Physically CREATE Materialized view AS statement
class PhysicalCreateMatView : public PhysicalOperator {
public:
	static constexpr const PhysicalOperatorType TYPE = PhysicalOperatorType::CREATE_MATVIEW;

public:
	PhysicalCreateMatView(LogicalOperator &op, SchemaCatalogEntry &schema,
	                               unique_ptr<BoundCreateTableInfo> info, idx_t estimated_cardinality);

	//! Schema to insert to
	SchemaCatalogEntry &schema;
	//! Table name to create
	unique_ptr<BoundCreateTableInfo> info;
public:
	// Source interface
	SourceResultType GetData(ExecutionContext &context, DataChunk &chunk, OperatorSourceInput &input) const override;

	bool IsSource() const override {
		return true;
	}
};
} // namespace duckdb
