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
class PhysicalCopy;

class BufferedCSVReader {
	static constexpr index_t INITIAL_BUFFER_SIZE = 16384;
	constexpr static index_t MAXIMUM_CSV_LINE_SIZE = 1048576;
public:
	BufferedCSVReader(PhysicalCopy &copy, std::istream &source);

	PhysicalCopy &copy;
	std::istream &source;

	unique_ptr<std::istream> csv_stream;

	unique_ptr<char[]> buffer;
	index_t buffer_size;
	index_t position;

	index_t linenr = 0;
	index_t nr_elements = 0;
	index_t total = 0;

	vector<unique_ptr<char[]>> cached_buffers;

	DataChunk insert_chunk;
	DataChunk parse_chunk;
	// FIXME
	vector<index_t> column_oids;
	vector<bool> set_to_default;
public:
	void ParseCSV(ClientContext &context);

	void CheckDelimiters(ClientContext &context, index_t &start, index_t position);

	bool ReadBuffer(index_t &start);
private:
	void AddValue(char *str_val, index_t length, index_t &column);
	void AddRow(ClientContext &context, index_t &column);
	void Flush(ClientContext &context);
};

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
};
} // namespace duckdb
