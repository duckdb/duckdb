//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/execution/operator/persistent/buffered_csv_reader.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/execution/operator/persistent/base_csv_reader.hpp"
#include "duckdb/execution/operator/persistent/csv_reader_options.hpp"
#include "duckdb/execution/operator/persistent/csv_file_handle.hpp"
#include "duckdb/execution/operator/persistent/csv_buffer.hpp"

#include <sstream>

namespace duckdb {
struct ReadCSVLocalState;

struct CSVBufferRead {
	CSVBufferRead(shared_ptr<CSVBuffer> buffer_p, idx_t buffer_start_p, idx_t buffer_end_p, idx_t batch_index,
	              idx_t estimated_linenr)
	    : buffer(move(buffer_p)), buffer_start(buffer_start_p), buffer_end(buffer_end_p), batch_index(batch_index),
	      estimated_linenr(estimated_linenr) {
		if (buffer) {
			if (buffer_end > buffer->GetBufferSize()) {
				buffer_end = buffer->GetBufferSize();
			}
		} else {
			buffer_start = 0;
			buffer_end = 0;
		}
	}

	CSVBufferRead() : buffer_start(0), buffer_end(NumericLimits<idx_t>::Maximum()) {};

	shared_ptr<CSVBuffer> buffer;

	idx_t buffer_start;
	idx_t buffer_end;
	idx_t batch_index;
	idx_t estimated_linenr;

public:
	void Reset() {
		buffer.reset();
		buffer_start = 0;
		buffer_end = NumericLimits<idx_t>::Maximum();
	}
};

//! Buffered CSV reader is a class that reads values from a stream and parses them as a CSV file
class ParallelCSVReader : public BaseCSVReader {
public:
	ParallelCSVReader(ClientContext &context, BufferedCSVReaderOptions options, const CSVBufferRead &buffer,
	                  const vector<LogicalType> &requested_types);
	~ParallelCSVReader();

	//! Current Position (Relative to the Buffer)
	idx_t position_buffer = 0;

	//! Start of the piece of the buffer this thread should read
	idx_t start_buffer = 0;
	//! End of the piece of this buffer this thread should read
	idx_t end_buffer = NumericLimits<idx_t>::Maximum();
	//! The actual buffer size
	idx_t buffer_size = 0;

	//! If this flag is set, it means we are about to try to read our last row.
	bool reached_remainder_state = false;

	char *buffer;

	CSVBufferRead buffer_read;

	idx_t position_set;

public:
	void SetBufferRead(const CSVBufferRead &buffer_read);
	//! Extract a single DataChunk from the CSV file and stores it in insert_chunk
	void ParseCSV(DataChunk &insert_chunk);

private:
	//! Initialize Parser
	void Initialize(const vector<LogicalType> &requested_types);
	//! Try to parse a single datachunk from the file. Throws an exception if anything goes wrong.
	void ParseCSV(ParserMode mode);
	//! Try to parse a single datachunk from the file. Returns whether or not the parsing is successful
	bool TryParseCSV(ParserMode mode);
	//! Extract a single DataChunk from the CSV file and stores it in insert_chunk
	bool TryParseCSV(ParserMode mode, DataChunk &insert_chunk, string &error_message);
	//! Sets Position depending on the byte_start of this thread
	bool SetPosition();
	//! When a buffer finishes reading its piece, it still can try to scan up to the real end of the buffer
	//! Up to finding a new line. This function sets the buffer_end and marks a boolean variable
	//! when changing the buffer end the first time.
	//! It returns FALSE if the parser should jump to the final state of parsing or not
	bool BufferRemainder();
	//! Parses a CSV file with a one-byte delimiter, escape and quote character
	bool TryParseSimpleCSV(DataChunk &insert_chunk, string &error_message);
};

} // namespace duckdb
