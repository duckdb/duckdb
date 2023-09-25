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

struct VerificationPositions {
	idx_t beginning_of_first_line = 0;
	idx_t end_of_last_line = 0;
};

//! The CSV Scanner is what iterates over CSV Buffers
class CSVScanner {
public:
	//! Constructor used when sniffing
	explicit CSVScanner(shared_ptr<CSVBufferManager> buffer_manager_p, unique_ptr<CSVStateMachine> state_machine_p);
	//! Constructor used when parsing
	explicit CSVScanner(shared_ptr<CSVBufferManager> buffer_manager_p, unique_ptr<CSVStateMachine> state_machine_p,
	                    idx_t buffer_idx, idx_t start_buffer, idx_t end_buffer, idx_t scanner_id);

	//! This functions templates an operation over the CSV File
	template <class OP, class T>
	inline bool Process(CSVScanner &machine, T &result) {
		OP::Initialize(machine);
		//! If current buffer is not set we try to get a new one
		if (!cur_buffer_handle) {
			cur_pos = 0;
			if (cur_buffer_idx == 0) {
				cur_pos = buffer_manager->GetStartPos();
			}
			cur_buffer_handle = buffer_manager->GetBuffer(cur_file_idx, cur_buffer_idx++);
			D_ASSERT(cur_buffer_handle);
		}
		while (cur_buffer_handle) {
			char *buffer_handle_ptr = cur_buffer_handle->Ptr();
			while (cur_pos < cur_buffer_handle->actual_size) {
				if (OP::Process(machine, result, buffer_handle_ptr[cur_pos], cur_pos)) {
					//! Not-Done Processing the File, but the Operator is happy!
					OP::Finalize(machine, result);
					return false;
				}
				cur_pos++;
			}
			cur_buffer_handle = buffer_manager->GetBuffer(cur_file_idx, cur_buffer_idx++);
			cur_pos = 0;
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

	const idx_t initial_buffer_set = 0;
	const idx_t scanner_id = 0;

	//! Verifies if value is UTF8
	void VerifyUTF8();

	//! End of the piece of this buffer this thread should read
	idx_t end_buffer = NumericLimits<idx_t>::Maximum();

	//! Parses data into a parse_chunk (chunk where all columns are initially set to varchar)
	void Parse(DataChunk &parse_chunk, VerificationPositions &verification_positions, const vector<LogicalType> &types);

	idx_t GetBufferIndex();

	idx_t GetTotalRowsEmmited();

private:
	idx_t cur_pos = 0;
	idx_t cur_buffer_idx = 0;
	idx_t cur_file_idx = 0;
	shared_ptr<CSVBufferManager> buffer_manager;
	unique_ptr<CSVBufferHandle> cur_buffer_handle;
	unique_ptr<CSVStateMachine> state_machine;
	bool start_set = false;
	idx_t total_rows_emmited = 0;

	//! ------------- CSV Parsing -------------------//
	//! The following set of functions and variables are related to actual CSV Parsing
	//! Sets the start of a buffer. In Parallel CSV Reading, buffers can (and most likely will) start mid-line.
	//! This function walks the buffer until the first new valid line.
	bool SetStart(VerificationPositions &verification_positions, const vector<LogicalType> &types);
	//! Skips empty lines when reading the first buffer
	void SkipEmptyLines();
	//! Skips header when reading the first buffer
	void SkipHeader();

	//! Start of the piece of the buffer this thread should read
	idx_t start_buffer = 0;
};

} // namespace duckdb
