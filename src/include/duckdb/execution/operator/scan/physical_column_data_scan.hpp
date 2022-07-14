//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/execution/operator/scan/physical_column_data_scan.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/types/column_data_collection.hpp"
#include "duckdb/execution/physical_operator.hpp"

namespace duckdb {

//! The PhysicalColumnDataScan scans a ColumnDataCollection
class PhysicalColumnDataScan : public PhysicalOperator {
public:
	PhysicalColumnDataScan(vector<LogicalType> types, PhysicalOperatorType op_type, idx_t estimated_cardinality)
	    : PhysicalOperator(op_type, move(types), estimated_cardinality), collection(nullptr) {
	}

	// the column data collection to scan
	ColumnDataCollection *collection;
	//! Owned column data collection, if any
	unique_ptr<ColumnDataCollection> owned_collection;

public:
	unique_ptr<GlobalSourceState> GetGlobalSourceState(ClientContext &context) const override;
	void GetData(ExecutionContext &context, DataChunk &chunk, GlobalSourceState &gstate,
	             LocalSourceState &lstate) const override;

public:
	void BuildPipelines(Executor &executor, Pipeline &current, PipelineBuildState &state) override;
};

} // namespace duckdb
