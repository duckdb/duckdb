//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/execution/operator/set/physical_cte.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/types/column/column_data_collection.hpp"
#include "duckdb/execution/physical_operator.hpp"

namespace duckdb {

class RecursiveCTEState;

class PhysicalCTE : public PhysicalOperator {
public:
	static constexpr const PhysicalOperatorType TYPE = PhysicalOperatorType::CTE;

public:
	PhysicalCTE(string ctename, idx_t table_index, vector<LogicalType> types, unique_ptr<PhysicalOperator> top,
	            unique_ptr<PhysicalOperator> bottom, idx_t estimated_cardinality);
	~PhysicalCTE() override;

	std::shared_ptr<ColumnDataCollection> working_table;
	shared_ptr<MetaPipeline> recursive_meta_pipeline;

	idx_t table_index;
	string ctename;

public:
	// Source interface
	SourceResultType GetData(ExecutionContext &context, DataChunk &chunk, OperatorSourceInput &input) const override;

	bool IsSource() const override {
		return true;
	}

public:
	// Sink interface
	SinkResultType Sink(ExecutionContext &context, DataChunk &chunk, OperatorSinkInput &input) const override;

	unique_ptr<GlobalSinkState> GetGlobalSinkState(ClientContext &context) const override;

	bool IsSink() const override {
		return true;
	}

	string ParamsToString() const override;

public:
	void BuildPipelines(Pipeline &current, MetaPipeline &meta_pipeline) override;

	vector<const_reference<PhysicalOperator>> GetSources() const override;

private:
	void ExecuteRecursivePipelines(ExecutionContext &context) const;
};

} // namespace duckdb
