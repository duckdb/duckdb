//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/execution/operator/persistent/physical_copy_to_file.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/execution/physical_operator.hpp"
#include "duckdb/parser/parsed_data/copy_info.hpp"

namespace duckdb {

//! Copy the contents of a query into a table
class PhysicalCopyToFile : public PhysicalOperator {
public:
	PhysicalCopyToFile(LogicalOperator &op, unique_ptr<CopyInfo> info)
	    : PhysicalOperator(PhysicalOperatorType::COPY_TO_FILE, op.types), info(move(info)) {
	}

	//! Settings for the COPY statement
	unique_ptr<CopyInfo> info;
	//! The names of the child expression
	vector<string> names;
	//! The types of the child expression
	vector<SQLType> sql_types;

public:
	void GetChunkInternal(ClientContext &context, DataChunk &chunk, PhysicalOperatorState *state) override;
};
} // namespace duckdb
