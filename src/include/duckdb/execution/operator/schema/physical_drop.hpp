//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/execution/operator/schema/physical_drop.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/execution/physical_operator.hpp"
#include "duckdb/parser/parsed_data/drop_info.hpp"

namespace duckdb {

//! PhysicalDrop represents a DROP [...] command
class PhysicalDrop : public PhysicalOperator {
public:
	explicit PhysicalDrop(unique_ptr<DropInfo> info, idx_t estimated_cardinality)
	    : PhysicalOperator(PhysicalOperatorType::DROP, {LogicalType::BOOLEAN}, estimated_cardinality),
	      info(move(info)) {
	}

	unique_ptr<DropInfo> info;

public:
	void GetChunkInternal(ExecutionContext &context, DataChunk &chunk, PhysicalOperatorState *state) const override;
};

} // namespace duckdb
