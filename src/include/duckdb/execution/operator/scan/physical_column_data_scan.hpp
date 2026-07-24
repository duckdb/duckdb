//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/execution/operator/scan/physical_column_data_scan.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/optionally_owned_ptr.hpp"
#include "duckdb/common/types/column/column_data_collection.hpp"
#include "duckdb/execution/physical_operator.hpp"

namespace duckdb {

//! The PhysicalColumnDataScan scans a ColumnDataCollection
class PhysicalColumnDataScan : public PhysicalOperator {
public:
	static constexpr const PhysicalOperatorType TYPE = PhysicalOperatorType::INVALID;

public:
	PhysicalColumnDataScan(PhysicalPlan &physical_plan, vector<LogicalType> types, PhysicalOperatorType op_type,
	                       idx_t estimated_cardinality, optionally_owned_ptr<ColumnDataCollection> collection);
	PhysicalColumnDataScan(PhysicalPlan &physical_plan, vector<LogicalType> types, PhysicalOperatorType op_type,
	                       idx_t estimated_cardinality, optionally_owned_ptr<ColumnDataCollection> collection,
	                       vector<column_t> column_ids);

	PhysicalColumnDataScan(PhysicalPlan &physical_plan, vector<LogicalType> types, PhysicalOperatorType op_type,
	                       idx_t estimated_cardinality, TableIndex cte_index);

	//! (optionally owned) column data collection to scan
	optionally_owned_ptr<ColumnDataCollection> collection;
	optional_ptr<PhysicalOperator> cte_source;
	//! Column ids scanned from the underlying collection
	vector<column_t> column_ids;

	TableIndex cte_index;
	optional_idx delim_index;

public:
	unique_ptr<GlobalSourceState> GetGlobalSourceState(ClientContext &context) const override;
	unique_ptr<GlobalSourceState> GetGlobalSourceState(ClientContext &context,
	                                                   const OperatorPartitionInfo &partition_info) const override;
	unique_ptr<LocalSourceState> GetLocalSourceState(ExecutionContext &context,
	                                                 GlobalSourceState &gstate) const override;
	SourceResultType GetDataInternal(ExecutionContext &context, DataChunk &chunk,
	                                 OperatorSourceInput &input) const override;
	ProgressData GetProgress(ClientContext &context, GlobalSourceState &gstate) const override;
	OperatorPartitionData GetPartitionData(ExecutionContext &context, DataChunk &chunk, GlobalSourceState &gstate,
	                                       LocalSourceState &lstate,
	                                       const OperatorPartitionInfo &partition_info) const override;

	bool IsSource() const override {
		return true;
	}
	bool SupportsPartitioning(const OperatorPartitionInfo &partition_info) const override;

	OrderPreservationType SourceOrder() const override {
		return source_order;
	}

	InsertionOrderPreservingMap<string> ParamsToString() const override;
	bool ParallelSource() const override {
		return true;
	}

	OrderPreservationType source_order = OrderPreservationType::INSERTION_ORDER;

public:
	void BuildPipelines(Pipeline &current, MetaPipeline &meta_pipeline) override;
};

} // namespace duckdb
