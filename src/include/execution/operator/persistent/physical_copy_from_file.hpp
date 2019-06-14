//===----------------------------------------------------------------------===//
//                         DuckDB
//
// execution/operator/persistent/physical_copy_from_file.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "execution/physical_operator.hpp"
#include "parser/parsed_data/copy_info.hpp"

namespace duckdb {

//! Parse a CSV file and copy the contents into a table
class PhysicalCopyFromFile : public PhysicalOperator {
public:
	PhysicalCopyFromFile(LogicalOperator &op, TableCatalogEntry *table, unique_ptr<CopyInfo> info)
	    : PhysicalOperator(PhysicalOperatorType::COPY_FROM_FILE, op.types), table(table), info(move(info)) {
	}

	void GetChunkInternal(ClientContext &context, DataChunk &chunk, PhysicalOperatorState *state) override;

	//! The table to copy into
	TableCatalogEntry *table;
	//! Settings for the COPY statement
	unique_ptr<CopyInfo> info;
};
} // namespace duckdb
