//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/execution/operator/join/physical_cross_product.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/execution/physical_operator.hpp"
#include "duckdb/common/types/column_data_collection.hpp"

namespace duckdb {

//! PhysicalCrossProduct represents a cross product between two tables
class PhysicalCrossProduct : public PhysicalOperator {
public:
	PhysicalCrossProduct(vector<LogicalType> types, unique_ptr<PhysicalOperator> left,
	                     unique_ptr<PhysicalOperator> right, idx_t estimated_cardinality);

public:
	// Operator Interface
	unique_ptr<OperatorState> GetOperatorState(ExecutionContext &context) const override;
	OperatorResultType Execute(ExecutionContext &context, DataChunk &input, DataChunk &chunk,
	                           GlobalOperatorState &gstate, OperatorState &state) const override;

	bool ParallelOperator() const override {
		return true;
	}

	bool RequiresCache() const override {
		return true;
	}

public:
	// Sink Interface
	unique_ptr<GlobalSinkState> GetGlobalSinkState(ClientContext &context) const override;
	SinkResultType Sink(ExecutionContext &context, GlobalSinkState &state, LocalSinkState &lstate,
	                    DataChunk &input) const override;

	bool IsSink() const override {
		return true;
	}
	bool ParallelSink() const override {
		return true;
	}

public:
	void BuildPipelines(Executor &executor, Pipeline &current, PipelineBuildState &state) override;
	vector<const PhysicalOperator *> GetSources() const override;
};

class CrossProductExecutor {
public:
	CrossProductExecutor(ColumnDataCollection &rhs);

	OperatorResultType Execute(DataChunk &input, DataChunk &output);

	bool ScanLHS() {
		return scan_input_chunk;
	}

	idx_t PositionInChunk() {
		return position_in_chunk;
	}

	idx_t ScanPosition() {
		return scan_state.current_row_index;
	}

private:
	void Reset(DataChunk &input, DataChunk &output);
	bool NextValue(DataChunk &input, DataChunk &output);

private:
	ColumnDataCollection &rhs;
	ColumnDataScanState scan_state;
	DataChunk scan_chunk;
	idx_t position_in_chunk;
	bool initialized;
	bool finished;
	bool scan_input_chunk;
};

} // namespace duckdb
