//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/execution/operator/join/physical_cross_product.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/types/column/column_data_collection.hpp"
#include "duckdb/execution/physical_operator.hpp"

namespace duckdb {

//! PhysicalCrossProduct represents a cross product between two tables
class PhysicalCrossProduct : public CachingPhysicalOperator {
public:
	static constexpr const PhysicalOperatorType TYPE = PhysicalOperatorType::CROSS_PRODUCT;

public:
	PhysicalCrossProduct(vector<LogicalType> types, unique_ptr<PhysicalOperator> left,
	                     unique_ptr<PhysicalOperator> right, idx_t estimated_cardinality);

public:
	// Operator Interface
	unique_ptr<OperatorState> GetOperatorState(ExecutionContext &context) const override;

	OrderPreservationType OperatorOrder() const override {
		return OrderPreservationType::NO_ORDER;
	}
	bool ParallelOperator() const override {
		return true;
	}

protected:
	OperatorResultType ExecuteInternal(ExecutionContext &context, DataChunk &input, DataChunk &chunk,
	                                   GlobalOperatorState &gstate, OperatorState &state) const override;

public:
	// Sink Interface
	unique_ptr<GlobalSinkState> GetGlobalSinkState(ClientContext &context) const override;
	SinkResultType Sink(ExecutionContext &context, DataChunk &chunk, OperatorSinkInput &input) const override;

	bool IsSink() const override {
		return true;
	}
	bool ParallelSink() const override {
		return true;
	}
	bool SinkOrderDependent() const override {
		return false;
	}

public:
	void BuildPipelines(Pipeline &current, MetaPipeline &meta_pipeline) override;
	vector<const_reference<PhysicalOperator>> GetSources() const override;
};

class CrossProductExecutor {
public:
	explicit CrossProductExecutor(ColumnDataCollection &rhs);

	OperatorResultType Execute(DataChunk &input, DataChunk &output);

	// returns if the left side is scanned as a constant vector
	bool ScanLHS() {
		return scan_input_chunk;
	}

	// returns the position in the chunk of chunk scanned as a constant input vector
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
