//===----------------------------------------------------------------------===//
//                         DuckDB
//
// execution/operator/persistent/physical_copy.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "execution/physical_operator.hpp"
#include "parser/parsed_data/copy_info.hpp"

namespace duckdb {

//! Physically copy file into a table
class PhysicalCopy : public PhysicalOperator {
public:
	PhysicalCopy(LogicalOperator &op, TableCatalogEntry *table, unique_ptr<CopyInfo> info)
	    : PhysicalOperator(PhysicalOperatorType::COPY, op.types), table(table), info(move(info)) {
	}

	PhysicalCopy(LogicalOperator &op, unique_ptr<CopyInfo> info)
	    : PhysicalOperator(PhysicalOperatorType::COPY, op.types), table(nullptr), info(move(info)) {
	}

	void GetChunkInternal(ClientContext &context, DataChunk &chunk, PhysicalOperatorState *state) override;

	//! The table to copy into (only for COPY FROM)
	TableCatalogEntry *table;
	//! Settings for the COPY statement
	unique_ptr<CopyInfo> info;
	//! The names of the child expression (only for COPY TO)
	vector<string> names;

private:
	vector<uint64_t> select_list_oid;
	vector<bool> set_to_default;

	void PushValue(string &line, DataChunk &insert_chunk, int64_t start, int64_t end, int64_t &column, int64_t linenr);
	void Flush(ClientContext &context, DataChunk &chunk, int64_t &nr_elements, int64_t &total,
	           vector<bool> &set_to_default);
};
} // namespace duckdb
