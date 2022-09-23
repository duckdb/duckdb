//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/execution/operator/persistent/physical_export.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include <utility>

#include "duckdb/execution/physical_operator.hpp"
#include "duckdb/function/copy_function.hpp"
#include "duckdb/parser/parsed_data/copy_info.hpp"
#include "duckdb/parser/parsed_data/exported_table_data.hpp"

namespace duckdb {
//! Parse a file from disk using a specified copy function and return the set of chunks retrieved from the file
class PhysicalExport : public PhysicalOperator {
public:
	PhysicalExport(vector<LogicalType> types, CopyFunction function, unique_ptr<CopyInfo> info,
	               idx_t estimated_cardinality, BoundExportData exported_tables)
	    : PhysicalOperator(PhysicalOperatorType::EXPORT, move(types), estimated_cardinality),
	      function(std::move(function)), info(move(info)), exported_tables(move(exported_tables)) {
	}

	//! The copy function to use to read the file
	CopyFunction function;
	//! The binding info containing the set of options for reading the file
	unique_ptr<CopyInfo> info;
	//! The table info for each table that will be exported
	BoundExportData exported_tables;

public:
	// Source interface
	unique_ptr<GlobalSourceState> GetGlobalSourceState(ClientContext &context) const override;
	void GetData(ExecutionContext &context, DataChunk &chunk, GlobalSourceState &gstate,
	             LocalSourceState &lstate) const override;

public:
	// Sink interface
	SinkResultType Sink(ExecutionContext &context, GlobalSinkState &gstate, LocalSinkState &lstate,
	                    DataChunk &input) const override;

	bool ParallelSink() const override {
		return true;
	}
	bool IsSink() const override {
		return true;
	}

public:
	void BuildPipelines(Executor &executor, Pipeline &current, PipelineBuildState &state) override;
	vector<const PhysicalOperator *> GetSources() const override;
};

} // namespace duckdb
