//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/execution/operator/set/physical_recursive_cte.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/execution/physical_operator.hpp"
#include "duckdb/common/types/column_data_collection.hpp"

namespace duckdb {
class Pipeline;
class RecursiveCTEState;

class PhysicalRecursiveCTE : public PhysicalOperator {
public:
	PhysicalRecursiveCTE(vector<LogicalType> types, bool union_all, unique_ptr<PhysicalOperator> top,
	                     unique_ptr<PhysicalOperator> bottom, idx_t estimated_cardinality);
	~PhysicalRecursiveCTE() override;

	bool union_all;
	std::shared_ptr<ColumnDataCollection> working_table;
	vector<shared_ptr<Pipeline>> pipelines;

public:
	// Source interface
	void GetData(ExecutionContext &context, DataChunk &chunk, GlobalSourceState &gstate,
	             LocalSourceState &lstate) const override;

public:
	// Sink interface
	SinkResultType Sink(ExecutionContext &context, GlobalSinkState &state, LocalSinkState &lstate,
	                    DataChunk &input) const override;

	unique_ptr<GlobalSinkState> GetGlobalSinkState(ClientContext &context) const override;

	bool IsSink() const override {
		return true;
	}

public:
	void BuildPipelines(Executor &executor, Pipeline &current, PipelineBuildState &state) override;

	vector<const PhysicalOperator *> GetSources() const override;

private:
	//! Probe Hash Table and eliminate duplicate rows
	idx_t ProbeHT(DataChunk &chunk, RecursiveCTEState &state) const;

	void ExecuteRecursivePipelines(ExecutionContext &context) const;
};

} // namespace duckdb
