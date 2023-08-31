//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/execution/operator/scan/csv/parallel_csv_reader.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/execution/operator/scan/csv/base_csv_reader.hpp"
#include "duckdb/execution/operator/scan/csv/csv_reader_options.hpp"
#include "duckdb/execution/operator/scan/csv/csv_file_handle.hpp"
#include "duckdb/execution/operator/scan/csv/csv_buffer.hpp"
#include "duckdb/execution/operator/scan/csv/csv_line_info.hpp"

#include <sstream>
#include <utility>

namespace duckdb {

struct CSVBufferRead {
	CSVBufferRead(unique_ptr<CSVBufferHandle> buffer_p, idx_t buffer_start_p, idx_t buffer_end_p, idx_t batch_index,
	              idx_t local_batch_index_p, optional_ptr<LineInfo> line_info_p)
	    : buffer(std::move(buffer_p)), line_info(line_info_p), buffer_start(buffer_start_p), buffer_end(buffer_end_p),
	      batch_index(batch_index), local_batch_index(local_batch_index_p) {
		D_ASSERT(buffer);
		if (buffer_end > buffer->actual_size) {
			buffer_end = buffer->actual_size;
		}
	}

	CSVBufferRead(unique_ptr<CSVBufferHandle> buffer_p, unique_ptr<CSVBufferHandle> nxt_buffer_p, idx_t buffer_start_p,
	              idx_t buffer_end_p, idx_t batch_index, idx_t local_batch_index, optional_ptr<LineInfo> line_info_p)
	    : CSVBufferRead(std::move(buffer_p), buffer_start_p, buffer_end_p, batch_index, local_batch_index,
	                    line_info_p) {
		next_buffer = std::move(nxt_buffer_p);
	}

	CSVBufferRead() : buffer_start(0), buffer_end(NumericLimits<idx_t>::Maximum()) {};

	const char &operator[](size_t i) const {
		if (i < buffer->actual_size) {
			auto buffer_ptr = buffer->Ptr();
			return buffer_ptr[i];
		}
		auto next_ptr = next_buffer->Ptr();
		return next_ptr[i - buffer->actual_size];
	}

	string_t GetValue(idx_t start_buffer, idx_t position_buffer, idx_t offset) {
		idx_t length = position_buffer - start_buffer - offset;
		// 1) It's all in the current buffer
		if (start_buffer + length <= buffer->actual_size) {
			auto buffer_ptr = buffer->Ptr();
			return string_t(buffer_ptr + start_buffer, length);
		} else if (start_buffer >= buffer->actual_size) {
			// 2) It's all in the next buffer
			D_ASSERT(next_buffer);
			D_ASSERT(next_buffer->actual_size >= length + (start_buffer - buffer->actual_size));
			auto buffer_ptr = next_buffer->Ptr();
			return string_t(buffer_ptr + (start_buffer - buffer->actual_size), length);
		} else {
			// 3) It starts in the current buffer and ends in the next buffer
			D_ASSERT(next_buffer);
			auto intersection = make_unsafe_uniq_array<char>(length);
			idx_t cur_pos = 0;
			auto buffer_ptr = buffer->Ptr();
			for (idx_t i = start_buffer; i < buffer->actual_size; i++) {
				intersection[cur_pos++] = buffer_ptr[i];
			}
			idx_t nxt_buffer_pos = 0;
			auto next_buffer_ptr = next_buffer->Ptr();
			for (; cur_pos < length; cur_pos++) {
				intersection[cur_pos] = next_buffer_ptr[nxt_buffer_pos++];
			}
			intersections.emplace_back(std::move(intersection));
			return string_t(intersections.back().get(), length);
		}
	}

	unique_ptr<CSVBufferHandle> buffer;
	unique_ptr<CSVBufferHandle> next_buffer;
	vector<unsafe_unique_array<char>> intersections;
	optional_ptr<LineInfo> line_info;

	idx_t buffer_start;
	idx_t buffer_end;
	idx_t batch_index;
	idx_t local_batch_index;
	idx_t lines_read = 0;
};

struct VerificationPositions {
	idx_t beginning_of_first_line = 0;
	idx_t end_of_last_line = 0;
};

//! CSV Reader for Parallel Reading
class ParallelCSVReader : public BaseCSVReader {
public:
	ParallelCSVReader(ClientContext &context, CSVReaderOptions options, unique_ptr<CSVBufferRead> buffer,
	                  idx_t first_pos_first_buffer, const vector<LogicalType> &requested_types, idx_t file_idx_p);
	virtual ~ParallelCSVReader() {
	}

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

	bool finished = false;

	unique_ptr<CSVBufferRead> buffer;

	idx_t file_idx;

	VerificationPositions GetVerificationPositions();

	//! Position of the first read line and last read line for verification purposes
	VerificationPositions verification_positions;

public:
	void SetBufferRead(unique_ptr<CSVBufferRead> buffer);
	//! Extract a single DataChunk from the CSV file and stores it in insert_chunk
	void ParseCSV(DataChunk &insert_chunk);

	idx_t GetLineError(idx_t line_error, idx_t buffer_idx, bool stop_at_first = true) override;

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
	//! Called when scanning the 1st buffer, skips empty lines
	void SkipEmptyLines();
	//! When a buffer finishes reading its piece, it still can try to scan up to the real end of the buffer
	//! Up to finding a new line. This function sets the buffer_end and marks a boolean variable
	//! when changing the buffer end the first time.
	//! It returns FALSE if the parser should jump to the final state of parsing or not
	bool BufferRemainder();

	bool NewLineDelimiter(bool carry, bool carry_followed_by_nl, bool first_char);

	//! Parses a CSV file with a one-byte delimiter, escape and quote character
	bool TryParseSimpleCSV(DataChunk &insert_chunk, string &error_message, bool try_add_line = false);

	//! First Position of First Buffer
	idx_t first_pos_first_buffer = 0;
};

} // namespace duckdb
