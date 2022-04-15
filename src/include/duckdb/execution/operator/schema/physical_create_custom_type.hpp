//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/execution/operator/schema/physical_create_custom_type.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/execution/physical_operator.hpp"
#include "duckdb/parser/parsed_data/create_custom_type_info.hpp"

namespace duckdb {

//! PhysicalCreateCustomType represents a CREATE TYPE command
class PhysicalCreateCustomType : public PhysicalOperator {
public:
	explicit PhysicalCreateCustomType(unique_ptr<CreateCustomTypeInfo> info, idx_t estimated_cardinality);

	unique_ptr<CreateCustomTypeInfo> info;

public:
	// Source interface
	unique_ptr<GlobalSourceState> GetGlobalSourceState(ClientContext &context) const override;
	void GetData(ExecutionContext &context, DataChunk &chunk, GlobalSourceState &gstate,
	             LocalSourceState &lstate) const override;
};

} // namespace duckdb