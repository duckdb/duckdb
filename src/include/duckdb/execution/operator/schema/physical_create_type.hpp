//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/execution/operator/schema/physical_create_type.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/execution/physical_operator.hpp"
#include "duckdb/parser/parsed_data/create_type_info.hpp"

namespace duckdb {

//! PhysicalCreateType represents a CREATE TYPE command
class PhysicalCreateType : public PhysicalOperator {
public:
	explicit PhysicalCreateType(unique_ptr<CreateTypeInfo> info, idx_t estimated_cardinality);

	unique_ptr<CreateTypeInfo> info;

public:
	// Source interface
	unique_ptr<GlobalSourceState> GetGlobalSourceState(ClientContext &context) const override;
	void GetData(ExecutionContext &context, DataChunk &chunk, GlobalSourceState &gstate,
	             LocalSourceState &lstate) const override;
};

} // namespace duckdb
