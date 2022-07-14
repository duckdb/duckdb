//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/execution/operator/helper/physical_vacuum.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/execution/physical_operator.hpp"
#include "duckdb/parser/parsed_data/load_info.hpp"

namespace duckdb {

//! PhysicalLoad represents an extension LOAD operation
class PhysicalLoad : public PhysicalOperator {
public:
	explicit PhysicalLoad(unique_ptr<LoadInfo> info, idx_t estimated_cardinality)
	    : PhysicalOperator(PhysicalOperatorType::LOAD, {LogicalType::BOOLEAN}, estimated_cardinality),
	      info(move(info)) {
	}

	unique_ptr<LoadInfo> info;

public:
	// Source interface
	void GetData(ExecutionContext &context, DataChunk &chunk, GlobalSourceState &gstate,
	             LocalSourceState &lstate) const override;
};

} // namespace duckdb
