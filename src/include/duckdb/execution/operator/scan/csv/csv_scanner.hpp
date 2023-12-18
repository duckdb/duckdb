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
#include "duckdb/execution/operator/scan/csv/parsing/scanner_boundary.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/common/multi_file_reader.hpp"
#include "duckdb/common/fast_mem.hpp"
#include "duckdb/execution/operator/scan/csv/scanner/base_scanner.hpp"

namespace duckdb {

//! The different scanner types
enum ScannerType : uint8_t { DIALECT_DETECTION = 0, TYPE_DETECTION = 1, TYPE_REFINEMENT = 2, PARSING = 3 };

//! A CSV Scanner is what iterates over CSV Buffers.
//! They are capable of autonomously getting their buffers, and by using a CSV State Machine, they can parse it to
//! different structures In certain sniffing phases the can simply consist of counting the number of columns. While when
//! reading the file It can be creating the data in DuckDB Chunks.

//! Due to a CSV File being broken into multiple buffers, the CSV Scanner also needs to ensure that overbuffer values
//! are Properly read
class CSVScanner {
	explicit CSVScanner(shared_ptr<CSVBufferManager> buffer_manager_p, shared_ptr<CSVStateMachine> state_machine_p,
	                    ScannerBoundary boundaries, idx_t scanner_id);

	//! This functions templates an operation over the CSV File
	template <class OP, class T>
	inline bool Process(CSVScanner &machine, T &result) {
		if (csv_iterator.bytes_to_read == 0) {
			//! Nothing to process, as we exhausted the bytes we can process in this scanner
			return false;
		}
		//! If current buffer is not set we try to get a new one
		if (!cur_buffer_handle) {
			cur_buffer_handle = buffer_manager->GetBuffer(csv_iterator.file_idx, csv_iterator.buffer_idx++);
			D_ASSERT(cur_buffer_handle);
		}
		char *buffer_handle_ptr = nullptr;
		OP::Initialize(machine, csv_iterator.buffer_pos);
		while (cur_buffer_handle && csv_iterator.bytes_to_read > 0) {
			buffer_handle_ptr = cur_buffer_handle->Ptr();
			for (; csv_iterator.buffer_pos < cur_buffer_handle->actual_size; csv_iterator.buffer_pos++) {
				if (OP::Process(machine, result, buffer_handle_ptr[csv_iterator.buffer_pos], csv_iterator.buffer_pos)) {
					//					csv_iterator.buffer_pos++;
					//! Not-Done Processing the File, but the Operator is happy!
					OP::Finalize(machine, result);
					return false;
				}
				csv_iterator.bytes_to_read--;
				if (csv_iterator.bytes_to_read == 0) {
					csv_iterator.buffer_pos++;
					break;
				}
			}
			if (csv_iterator.bytes_to_read == 0) {
				break;
			}
			previous_cur_buffer_handle = std::move(cur_buffer_handle);
			cur_buffer_handle = buffer_manager->GetBuffer(csv_iterator.file_idx, csv_iterator.buffer_idx++);
			if (!cur_buffer_handle) {
				// we are done, no more bytes to read
				csv_iterator.bytes_to_read = 0;
			}
			csv_iterator.buffer_pos = 0;
			if (cur_buffer_handle) {
				buffer_handle_ptr = cur_buffer_handle->Ptr();
				ProcessOverbufferValue();
			}
		}
		//! We must ensure that process continues until a full line is read. If it stops in a newline we also continue
		//! it. This is regardless of bytes_to_read
		idx_t cur_val_pos = current_value_pos;
		idx_t before_addition = current_value_pos;
		if (states.NewRow()) {
			cur_val_pos++;
		}
		while ((cur_val_pos > current_value_pos || current_value_pos % total_columns != 0) && cur_buffer_handle) {
			D_ASSERT(csv_iterator.bytes_to_read == 0);
			if (csv_iterator.buffer_pos >= cur_buffer_handle->actual_size) {
				previous_cur_buffer_handle = std::move(cur_buffer_handle);
				cur_buffer_handle = buffer_manager->GetBuffer(csv_iterator.file_idx, csv_iterator.buffer_idx++);
				if (!cur_buffer_handle) {
					// we are done, no more bytes to read
					break;
				}
				csv_iterator.buffer_pos = 0;
				buffer_handle_ptr = cur_buffer_handle->Ptr();
				ProcessOverbufferValue();
			}
			if (OP::Process(machine, result, buffer_handle_ptr[csv_iterator.buffer_pos], csv_iterator.buffer_pos)) {

				//! Not-Done Processing the File, but the Operator is happy!
				OP::Finalize(machine, result);
				return false;
			}
			csv_iterator.buffer_pos++;
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

	//! Current position on values
	idx_t current_value_pos = 0;

	idx_t length = 0;

	idx_t cur_rows = 0;
	idx_t column_count = 1;

	CSVStates states;

	//! String Values per [row|column]
	unique_ptr<CSVValue[]> values;
	unique_ptr<Vector> duck_vector;
	string_t *duck_vector_ptr;
	idx_t values_size;

	vector<string_t *> parse_data;

	string value;

	idx_t rows_read = 0;
	idx_t line_start_pos = 0;

	//! Id of the scanner, used to know order in which data is in the CSV file(s)
	const idx_t scanner_id = 0;

	idx_t forgotten_lines = 0;

	bool Flush(DataChunk &insert_chunk, idx_t buffer_idx, bool try_add_line);

	//! Parses data into a output_chunk
	void Parse(DataChunk &output_chunk, VerificationPositions &verification_positions);

	void Process();
	//! Produces error messages for column name -> type mismatch.
	static string ColumnTypesError(case_insensitive_map_t<idx_t> sql_types_per_column, const vector<string> &names);

	//! Gets the current buffer index of this scanner. Returns -1 if scanner has no buffer attached to it.
	int64_t GetBufferIndex();

	//! Gets the total rows emmited by this scanner.
	//! This is currently used for retrieving lines when errors occur.
	idx_t GetTotalRowsEmmited();

	const string &GetFileName() {
		return file_path;
	}
	const vector<string> &GetNames() {
		return names;
	}
	const vector<LogicalType> &GetTypes() {
		return types;
	}

	MultiFileReaderData reader_data;
	string file_path;
	vector<string> names;
	vector<LogicalType> types;

	bool Last();
	//! Unique pointer to the buffer_handle, this is unique per scanner, since it also contains the necessary counters
	//! To offload buffers to disk if necessary
	unique_ptr<CSVBufferHandle> cur_buffer_handle;

	unique_ptr<CSVBufferHandle> previous_cur_buffer_handle;
	//! Parse Chunk where all columns are defined as VARCHAR
	DataChunk parse_chunk;

	//! Total Number of Columns
	idx_t total_columns = 0;

	vector<SelectionVector> selection_vectors;

	void SetTotalColumns(idx_t total_columns_p) {
		total_columns = total_columns_p;
		vector<LogicalType> varchar_types(total_columns, LogicalType::VARCHAR);
		values_size = total_columns * STANDARD_VECTOR_SIZE;
		duck_vector = make_uniq<Vector>(LogicalType::VARCHAR, values_size);

		selection_vectors.resize(total_columns);

		// precompute these selection vectors
		for (idx_t i = 0; i < selection_vectors.size(); i++) {
			selection_vectors[i].Initialize();
			for (idx_t j = 0; j < STANDARD_VECTOR_SIZE; j++) {
				selection_vectors[i][j] = i + (total_columns * j);
			}
		}

		parse_chunk.Initialize(BufferAllocator::Get(buffer_manager->context), varchar_types);
	}

private:
	//! Where this CSV Scanner starts
	CSVIterator csv_iterator;
	//! Shared pointer to the buffer_manager, this is shared across multiple scanners
	shared_ptr<CSVBufferManager> buffer_manager;

	//! Shared pointer to the state machine, this is used across multiple scanners
	shared_ptr<CSVStateMachine> state_machine;

	const ParserMode mode;
	//! ------------- CSV Parsing -------------------//
	//! The following set of functions and variables are related to actual CSV Parsing
	//! Sets the start of a buffer. In Parallel CSV Reading, buffers can (and most likely will) start mid-line.

	//! If we already set the start of this CSV Scanner (i.e., the next newline)
	bool start_set = false;
	//! Number of rows emmited by this scanner
	idx_t total_rows_emmited = 0;

	//! This function walks the buffer until the first new valid line.
	bool SetStart(VerificationPositions &verification_positions);
	//! Skips empty lines when reading the first buffer
	void SkipEmptyLines();
	//! Skips header when reading the first buffer
	void SkipHeader();
	//! Process values that fall over two buffers.
	void ProcessOverbufferValue();
};

} // namespace duckdb
