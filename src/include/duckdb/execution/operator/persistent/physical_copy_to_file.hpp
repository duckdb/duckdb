//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/execution/operator/persistent/physical_copy_to_file.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/execution/physical_sink.hpp"
#include "duckdb/parser/parsed_data/copy_info.hpp"
#include "duckdb/function/copy_function.hpp"

namespace duckdb {

//! Copy the contents of a query into a table
class PhysicalCopyToFile : public PhysicalSink {
public:
	PhysicalCopyToFile(vector<LogicalType> types, CopyFunction function, unique_ptr<FunctionData> bind_data,
	                   idx_t estimated_cardinality)
	    : PhysicalSink(PhysicalOperatorType::COPY_TO_FILE, move(types), estimated_cardinality), function(function),
	      bind_data(move(bind_data)) {
	}

	CopyFunction function;
	unique_ptr<FunctionData> bind_data;

public:
	void GetChunkInternal(ExecutionContext &context, DataChunk &chunk, PhysicalOperatorState *state) const override;

	void Sink(ExecutionContext &context, GlobalOperatorState &gstate, LocalSinkState &lstate,
	          DataChunk &input) const override;
	void Combine(ExecutionContext &context, GlobalOperatorState &gstate, LocalSinkState &lstate) override;
	bool Finalize(Pipeline &pipeline, ClientContext &context, unique_ptr<GlobalOperatorState> gstate) override;
	unique_ptr<LocalSinkState> GetLocalSinkState(ExecutionContext &context) override;
	unique_ptr<GlobalOperatorState> GetGlobalState(ClientContext &context) override;
};
} // namespace duckdb
