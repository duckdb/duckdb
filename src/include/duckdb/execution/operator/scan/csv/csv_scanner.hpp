//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/execution/operator/scan/csv/csv_scanner.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/execution/operator/scan/csv/csv_buffer_manager.hpp"
#include "duckdb/execution/operator/scan/csv/csv_file_handle.hpp"
#include "duckdb/execution/operator/scan/csv/csv_reader_options.hpp"
#include "duckdb/execution/operator/scan/csv/csv_state_machine.hpp"
#include "duckdb/main/client_context.hpp"

namespace duckdb {

//! Structure that holds information on the beginning of the first line and end of the last line read by this scanner
//! This is mainly used for verification and to guarantee our parallel scanner read all lines correctly
struct VerificationPositions {
	idx_t beginning_of_first_line = 0;
	idx_t end_of_last_line = 0;
};

//! Structure that holds information from the data a scanner should scan
struct CSVIterator {
	CSVIterator(idx_t file_idx_p, idx_t buffer_idx_p, idx_t buffer_pos_p, idx_t bytes_to_read_p)
	    : file_idx(file_idx_p), start_buffer_idx(buffer_idx_p), start_buffer_pos(buffer_pos_p),
	      buffer_idx(buffer_idx_p), buffer_pos(buffer_pos_p), bytes_to_read(bytes_to_read_p) {};
	//! Constructor used for the first CSV Iterator of a scanner
	CSVIterator(idx_t start_buffer_pos_p, idx_t bytes_to_read_p):
	      start_buffer_pos(start_buffer_pos_p), buffer_pos(start_buffer_pos_p),bytes_to_read(bytes_to_read_p) {};
	CSVIterator() {};

	//! Resets the Iterator, only used in the sniffing where scanners must be restarted for dialect/type detection
	void Reset();
	//! Moves the Iterator to the next positions
	//! There are three options for the iterator movement.
	//! 1) We are done with the current file, hence we move to the next file
	//! 2) We are done with the current buffer, hence we move to the next buffer
	//! 3) We are not done with the current buffer, hence we just move where we start within the buffer
	bool Next(CSVBufferManager& buffer_manager);

	//! File index where we start scanning [0-idx], a scanner can never go over one file.
	idx_t file_idx = 0;
	//! Start Buffer index of the file where we start scanning
	idx_t start_buffer_idx = 0;
	//! Start Buffer position of the buffer of the file where we start scanning
	idx_t start_buffer_pos = 0;

	//! Current Buffer index of the file we are scanning
	idx_t buffer_idx = 0;
	//! Current Buffer position of the buffer of the file we are scanning
	idx_t buffer_pos = 0;
	//! How many bytes this CSV Scanner should read
	//! If higher than the remainder of the file, will read the file in its entirety
	idx_t bytes_to_read = NumericLimits<idx_t>::Maximum();
	idx_t iterator_id = 0;
};

//! The CSV Scanner is what iterates over CSV Buffers
class CSVScanner {
public:
	//! Constructor used for result checking in unit-testing
	CSVScanner(ClientContext &context, CSVReaderOptions &options);
	//! Constructor used when sniffing
	explicit CSVScanner(shared_ptr<CSVBufferManager> buffer_manager_p, unique_ptr<CSVStateMachine> state_machine_p);
	//! Constructor used when parsing
	explicit CSVScanner(shared_ptr<CSVBufferManager> buffer_manager_p, unique_ptr<CSVStateMachine> state_machine_p,
	                    CSVIterator csv_iterator);

	//! This functions templates an operation over the CSV File
	template <class OP, class T>
	inline bool Process(CSVScanner &machine, T &result) {
		if (csv_iterator.bytes_to_read == 0) {
			//! Nothing to process, as we exhausted the bytes we can process in this scanner
			return false;
		}
		OP::Initialize(machine);
		//! If current buffer is not set we try to get a new one
		if (!cur_buffer_handle) {
			csv_iterator.buffer_pos = 0;
			if (csv_iterator.buffer_idx == 0) {
				csv_iterator.buffer_pos = buffer_manager->GetStartPos();
			}
			cur_buffer_handle = buffer_manager->GetBuffer(csv_iterator.file_idx, csv_iterator.buffer_idx++);
			D_ASSERT(cur_buffer_handle);
		}
		while (cur_buffer_handle) {
			char *buffer_handle_ptr = cur_buffer_handle->Ptr();
			while (csv_iterator.buffer_pos < cur_buffer_handle->actual_size) {
				if (OP::Process(machine, result, buffer_handle_ptr[csv_iterator.buffer_pos], csv_iterator.buffer_pos) ||
				    csv_iterator.bytes_to_read == 0) {
					//! Not-Done Processing the File, but the Operator is happy!
					OP::Finalize(machine, result);
					return false;
				}
				csv_iterator.buffer_pos++;
				csv_iterator.bytes_to_read--;
			}
			cur_buffer_handle = buffer_manager->GetBuffer(csv_iterator.file_idx, csv_iterator.buffer_idx++);
			csv_iterator.buffer_pos = 0;
		}
		//! Done Processing the File
		OP::Finalize(machine, result);
		return true;
	}
	//! Returns true if the iterator is finished
	bool Finished();
	//! Resets the iterator
	void Reset();

	CSVStateMachine &GetStateMachine();

	CSVStateMachineSniffing &GetStateMachineSniff();

	//! Current Numbers of Rows
	idx_t cur_rows = 0;
	//! Current Number of Columns
	idx_t column_count = 1;

	//! Current, previous, and state before the previous
	CSVState state;
	CSVState previous_state;
	CSVState pre_previous_state;

	//! String Value
	string value;
	idx_t rows_read = 0;
	idx_t line_start_pos = 0;

	//! Verifies if value is UTF8
	void VerifyUTF8();

	//! Parses data into a parse_chunk (chunk where all columns are initially set to varchar)
	void Parse(DataChunk &parse_chunk, VerificationPositions &verification_positions, const vector<LogicalType> &types);

	//! Produces error messages for column name -> type mismatch.
	static string ColumnTypesError(case_insensitive_map_t<idx_t> sql_types_per_column, const vector<string> &names);

	//! Gets the current buffer index of this scanner. Returns -1 if scanner has no buffer attached to it.
	int64_t GetBufferIndex();

	//! Gets the total rows emmited by this scanner.
	//! This is currently used for retrieving lines when errors occur.
	idx_t GetTotalRowsEmmited();

private:
	//! Where this CSV Scanner starts
	CSVIterator csv_iterator;
	//! Shared pointer to the buffer_manager, this is shared across multiple scanners
	shared_ptr<CSVBufferManager> buffer_manager;
	//! Unique pointer to the buffer_handle, this is unique per scanner, since it also contains the necessary counters
	//! To offload buffers to disk if necessary
	unique_ptr<CSVBufferHandle> cur_buffer_handle;
	//! Shared pointer to the state machine, this is used across multiple scanners
	shared_ptr<CSVStateMachine> state_machine;

	//! ------------- CSV Parsing -------------------//
	//! The following set of functions and variables are related to actual CSV Parsing
	//! Sets the start of a buffer. In Parallel CSV Reading, buffers can (and most likely will) start mid-line.

	//! If we already set the start of this CSV Scanner (i.e., the next newline)
	bool start_set = false;
	//! Number of rows emmited by this scanner
	idx_t total_rows_emmited = 0;
	//! This function walks the buffer until the first new valid line.
	bool SetStart(VerificationPositions &verification_positions, const vector<LogicalType> &types);
	//! Skips empty lines when reading the first buffer
	void SkipEmptyLines();
	//! Skips header when reading the first buffer
	void SkipHeader();
};

} // namespace duckdb
