//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/execution/operator/scan/physical_column_data_scan.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/types/column/column_data_collection.hpp"
#include "duckdb/execution/physical_operator.hpp"

namespace duckdb {

//! The PhysicalColumnDataScan scans a ColumnDataCollection
class PhysicalColumnDataScan : public PhysicalOperator {
public:
	static constexpr const PhysicalOperatorType TYPE = PhysicalOperatorType::COLUMN_DATA_SCAN;

public:
	PhysicalColumnDataScan(vector<LogicalType> types, PhysicalOperatorType op_type, idx_t estimated_cardinality,
	                       unique_ptr<ColumnDataCollection> owned_collection = nullptr);

	PhysicalColumnDataScan(vector<LogicalType> types, PhysicalOperatorType op_type, idx_t estimated_cardinality,
	                       idx_t cte_index)
	    : PhysicalOperator(op_type, std::move(types), estimated_cardinality), collection(nullptr),
	      cte_index(cte_index) {
	}

	// the column data collection to scan
	optional_ptr<ColumnDataCollection> collection;
	//! Owned column data collection, if any
	unique_ptr<ColumnDataCollection> owned_collection;

	idx_t cte_index;

public:
	unique_ptr<GlobalSourceState> GetGlobalSourceState(ClientContext &context) const override;
	SourceResultType GetData(ExecutionContext &context, DataChunk &chunk, OperatorSourceInput &input) const override;

	bool IsSource() const override {
		return true;
	}

	string ParamsToString() const override;

public:
	void BuildPipelines(Pipeline &current, MetaPipeline &meta_pipeline) override;
};

} // namespace duckdb
