//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/execution/operator/aggregate/physical_window.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/types/chunk_collection.hpp"
#include "duckdb/execution/physical_operator.hpp"
#include "duckdb/parallel/pipeline.hpp"

namespace duckdb {

//! PhysicalWindow implements window functions
//! It assumes that all functions have a common partitioning and ordering
class PhysicalWindow : public PhysicalOperator {
public:
	PhysicalWindow(vector<LogicalType> types, vector<unique_ptr<Expression>> select_list, idx_t estimated_cardinality,
	               PhysicalOperatorType type = PhysicalOperatorType::WINDOW);

	//! The projection list of the WINDOW statement (may contain aggregates)
	vector<unique_ptr<Expression>> select_list;

public:
	// Source interface
	unique_ptr<LocalSourceState> GetLocalSourceState(ExecutionContext &context, GlobalSourceState &gstate) const override;
	void GetData(ExecutionContext &context, DataChunk &chunk, GlobalSourceState &gstate, LocalSourceState &lstate) const override;

public:
	// Sink interface
	void Sink(ExecutionContext &context, GlobalSinkState &state, LocalSinkState &lstate,
	          DataChunk &input) const override;
	void Combine(ExecutionContext &context, GlobalSinkState &state, LocalSinkState &lstate) const override;
	bool Finalize(Pipeline &pipeline, ClientContext &context, unique_ptr<GlobalSinkState> gstate) override;
	bool FinalizeInternal(ClientContext &context, unique_ptr<GlobalSinkState> gstate);

	unique_ptr<LocalSinkState> GetLocalSinkState(ExecutionContext &context) const override;
	unique_ptr<GlobalSinkState> GetGlobalSinkState(ClientContext &context) const override;

	bool IsSink() const override {
		return true;
	}

public:
	idx_t MaxThreads(ClientContext &context);
	unique_ptr<ParallelState> GetParallelState();

	string ParamsToString() const override;
};

} // namespace duckdb
