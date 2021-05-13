//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/execution/operator/helper/physical_set.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/execution/physical_operator.hpp"
#include "duckdb/parser/parsed_data/vacuum_info.hpp"

namespace duckdb {

//! PhysicalSet represents a SET operation (e.g. SET a = 42)
class PhysicalSet : public PhysicalOperator {
public:
	PhysicalSet(std::string name_p, Value value_p, idx_t estimated_cardinality)
	    : PhysicalOperator(PhysicalOperatorType::SET, {LogicalType::BOOLEAN}, estimated_cardinality), name(name_p),
	      value(value_p) {
	}

public:
	void GetChunkInternal(ExecutionContext &context, DataChunk &chunk, PhysicalOperatorState *state) const override;

public:
	std::string name;
	Value value;
};

} // namespace duckdb
