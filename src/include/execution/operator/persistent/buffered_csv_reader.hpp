//===----------------------------------------------------------------------===//
//                         DuckDB
//
// execution/operator/persistent/buffered_csv_reader.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "execution/physical_operator.hpp"
#include "parser/parsed_data/copy_info.hpp"

namespace duckdb {
class PhysicalCopyFromFile;

//! Buffered CSV reader is a class that reads values from a stream and parses them as a CSV file
class BufferedCSVReader {
	static constexpr index_t INITIAL_BUFFER_SIZE = 16384;
	static constexpr index_t MAXIMUM_CSV_LINE_SIZE = 1048576;
public:
	BufferedCSVReader(PhysicalCopyFromFile &copy, std::istream &source);

	PhysicalCopyFromFile &copy;
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

}
