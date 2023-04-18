//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/execution/operator/set/physical_cte.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/types/column_data_collection.hpp"
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

	vector<PhysicalOperator *> cte_scans;

	std::shared_ptr<ColumnDataCollection> working_table;
	shared_ptr<MetaPipeline> recursive_meta_pipeline;

	idx_t table_index;
	string ctename;

public:
	bool IsSource() const override {
		return true;
	}

public:
	// Sink interface
	SinkResultType Sink(ExecutionContext &context, GlobalSinkState &state, LocalSinkState &lstate,
	                    DataChunk &input) const override;

	bool IsSink() const override {
		return true;
	}

	string ParamsToString() const override;

public:
	void BuildPipelines(Pipeline &current, MetaPipeline &meta_pipeline) override;

	vector<const PhysicalOperator *> GetSources() const override;
};

} // namespace duckdb
