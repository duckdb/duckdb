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
	//! The types of the child expression (only for COPY TO)
	vector<SQLType> sql_types;

private:
	vector<index_t> column_oids;
	vector<bool> set_to_default;

	void PushValue(char *line, DataChunk &insert_chunk, index_t start, index_t end, index_t &column, index_t linenr);
	void Flush(ClientContext &context, DataChunk &parse_chunk, DataChunk &chunk, count_t &nr_elements, count_t &total,
	           vector<bool> &set_to_default);
};
} // namespace duckdb
