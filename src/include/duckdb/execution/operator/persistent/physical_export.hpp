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

namespace duckdb {
//! Parse a file from disk using a specified copy function and return the set of chunks retrieved from the file
class PhysicalExport : public PhysicalOperator {
public:
	PhysicalExport(vector<LogicalType> types, CopyFunction function, unique_ptr<CopyInfo> info,
	               idx_t estimated_cardinality)
	    : PhysicalOperator(PhysicalOperatorType::EXPORT, move(types), estimated_cardinality),
	      function(std::move(function)), info(move(info)) {
	}

	//! The copy function to use to read the file
	CopyFunction function;
	//! The binding info containing the set of options for reading the file
	unique_ptr<CopyInfo> info;

public:
	void GetChunkInternal(ExecutionContext &context, DataChunk &chunk, PhysicalOperatorState *state) const override;
};

} // namespace duckdb
