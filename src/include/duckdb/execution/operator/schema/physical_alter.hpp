//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/execution/operator/schema/physical_alter.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/execution/physical_operator.hpp"
#include "duckdb/parser/parsed_data/alter_table_info.hpp"

namespace duckdb {

//! PhysicalAlter represents an ALTER TABLE command
class PhysicalAlter : public PhysicalOperator {
public:
	explicit PhysicalAlter(unique_ptr<AlterInfo> info, idx_t estimated_cardinality)
	    : PhysicalOperator(PhysicalOperatorType::ALTER, {LogicalType::BOOLEAN}, estimated_cardinality),
	      info(std::move(info)) {
	}

	unique_ptr<AlterInfo> info;

public:
	// Source interface
	unique_ptr<GlobalSourceState> GetGlobalSourceState(ClientContext &context) const override;
	void GetData(ExecutionContext &context, DataChunk &chunk, GlobalSourceState &gstate,
	             LocalSourceState &lstate) const override;
};

} // namespace duckdb
