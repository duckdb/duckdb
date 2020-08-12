//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/execution/operator/persistent/physical_copy_from_file.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/execution/physical_operator.hpp"
#include "duckdb/function/copy_function.hpp"

namespace duckdb {
class BufferedCSVReader;

//! Parse a file from disk using a specified copy function and return the set of chunks retrieved from the file
class PhysicalCopyFromFile : public PhysicalOperator {
public:
	PhysicalCopyFromFile(vector<TypeId> types, CopyFunction function, unique_ptr<FunctionData> info, vector<LogicalType> sql_types)
	    : PhysicalOperator(PhysicalOperatorType::COPY_FROM_FILE, move(types)), function(function), info(move(info)), sql_types(move(sql_types)) {
	}

	//! The copy function to use to read the file
	CopyFunction function;
	//! The binding info containing the set of options for reading the file
	unique_ptr<FunctionData> info;
	//! The set of types to retrieve from the file
	vector<LogicalType> sql_types;

public:
	void GetChunkInternal(ExecutionContext &context, DataChunk &chunk, PhysicalOperatorState *state) override;
	unique_ptr<PhysicalOperatorState> GetOperatorState() override;
};

} // namespace duckdb
