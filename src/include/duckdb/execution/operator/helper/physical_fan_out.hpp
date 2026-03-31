//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/execution/operator/helper/physical_fan_out.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/execution/physical_operator.hpp"

namespace duckdb {

//! PhysicalFanOut wraps a sequential source and exposes it as a parallel source.
//! Multiple threads call GetData concurrently; access to the underlying source is
//! serialized with a mutex. Each chunk gets a monotonically increasing batch index
//! so downstream operators (and the BatchCollector) can reconstruct original order.
class PhysicalFanOut : public PhysicalOperator {
public:
	static constexpr const PhysicalOperatorType TYPE = PhysicalOperatorType::FAN_OUT;

	PhysicalFanOut(PhysicalPlan &plan, PhysicalOperator &child_source, idx_t estimated_cardinality);

	//! The wrapped sequential source
	reference<PhysicalOperator> child_source;

public:
	// Source interface
	bool IsSource() const override {
		return true;
	}
	bool ParallelSource() const override {
		return true;
	}

	unique_ptr<GlobalSourceState> GetGlobalSourceState(ClientContext &context) const override;
	unique_ptr<LocalSourceState> GetLocalSourceState(ExecutionContext &context,
	                                                 GlobalSourceState &gstate) const override;
	SourceResultType GetDataInternal(ExecutionContext &context, DataChunk &chunk,
	                                 OperatorSourceInput &input) const override;

	bool SupportsPartitioning(const OperatorPartitionInfo &partition_info) const override {
		if (partition_info.RequiresBatchIndex()) {
			// FanOut always provides batch indices — it serializes the source and assigns them
			return true;
		}
		return child_source.get().SupportsPartitioning(partition_info);
	}
	OperatorPartitionData GetPartitionData(ExecutionContext &context, DataChunk &chunk, GlobalSourceState &gstate,
	                                       LocalSourceState &lstate,
	                                       const OperatorPartitionInfo &partition_info) const override;

	bool IsSink() const override {
		return false;
	}

	string GetName() const override {
		return "FAN_OUT";
	}

	InsertionOrderPreservingMap<string> ParamsToString() const override {
		InsertionOrderPreservingMap<string> result;
		result["Source"] = child_source.get().GetName();
		for (auto &entry : child_source.get().ParamsToString()) {
			result[entry.first] = entry.second;
		}
		return result;
	}
};

} // namespace duckdb
