//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/execution/operator/csv_scanner/base_scanner.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/execution/operator/csv_scanner/csv_buffer_manager.hpp"
#include "duckdb/execution/operator/csv_scanner/scanner_boundary.hpp"
#include "duckdb/execution/operator/csv_scanner/csv_state_machine.hpp"
#include "duckdb/execution/operator/csv_scanner/csv_error.hpp"

namespace duckdb {

class CSVFileScan;
class ScannerResult {
public:
	ScannerResult(CSVStates &states, CSVStateMachine &state_machine);

	//! Adds a Value to the result
	static inline void SetQuoted(ScannerResult &result, idx_t quoted_position) {
		if (!result.quoted) {
			result.quoted_position = quoted_position;
		}
		result.quoted = true;
	}
	//! Adds a Row to the result
	static inline void SetEscaped(ScannerResult &result) {
		result.escaped = true;
	}
	// Variable to keep information regarding quoted and escaped values
	bool quoted = false;
	bool escaped = false;
	idx_t quoted_position = 0;

protected:
	CSVStates &states;
	CSVStateMachine &state_machine;
};

//! This is the base of our CSV scanners.
//! Scanners differ on what they are used for, and consequently have different performance benefits.
class BaseScanner {
public:
	explicit BaseScanner(shared_ptr<CSVBufferManager> buffer_manager, shared_ptr<CSVStateMachine> state_machine,
	                     shared_ptr<CSVErrorHandler> error_handler, shared_ptr<CSVFileScan> csv_file_scan = nullptr,
	                     CSVIterator iterator = {});

	virtual ~BaseScanner() = default;
	//! Returns true if the scanner is finished
	bool FinishedFile();
	//! Resets the scanner
	void Reset();
	//! Parses data into a output_chunk
	virtual ScannerResult &ParseChunk();

	//! Returns the result from the last Parse call. Shouts at you if you call it wrong
	virtual ScannerResult &GetResult();

	CSVIterator &GetIterator();

	idx_t GetBoundaryIndex() {
		return iterator.GetBoundaryIdx();
	}

	idx_t GetLinesRead() {
		return lines_read;
	}

	idx_t GetIteratorPosition() {
		return iterator.pos.buffer_pos;
	}

	CSVStateMachine &GetStateMachine();

	shared_ptr<CSVFileScan> csv_file_scan;

	//! If this scanner is being used for sniffing
	bool sniffing = false;
	//! The guy that handles errors
	shared_ptr<CSVErrorHandler> error_handler;

	//! Shared pointer to the state machine, this is used across multiple scanners
	shared_ptr<CSVStateMachine> state_machine;

	//! States
	CSVStates states;

protected:
	//! Boundaries of this scanner
	CSVIterator iterator;

	//! Unique pointer to the buffer_handle, this is unique per scanner, since it also contains the necessary counters
	//! To offload buffers to disk if necessary
	shared_ptr<CSVBufferHandle> cur_buffer_handle;

	//! Hold the current buffer ptr
	char *buffer_handle_ptr = nullptr;

	//! Shared pointer to the buffer_manager, this is shared across multiple scanners
	shared_ptr<CSVBufferManager> buffer_manager;

	//! If this scanner has been initialized
	bool initialized = false;
	//! How many lines were read by this scanner
	idx_t lines_read = 0;
	idx_t bytes_read = 0;
	//! Internal Functions used to perform the parsing
	//! Initializes the scanner
	virtual void Initialize();

	//! Process one chunk
	template <class T>
	void Process(T &result) {
		idx_t to_pos;
		const idx_t start_pos = iterator.pos.buffer_pos;
		if (iterator.IsBoundarySet()) {
			to_pos = iterator.GetEndPos();
			if (to_pos > cur_buffer_handle->actual_size) {
				to_pos = cur_buffer_handle->actual_size;
			}
		} else {
			to_pos = cur_buffer_handle->actual_size;
		}
		while (iterator.pos.buffer_pos < to_pos) {
			state_machine->Transition(states, buffer_handle_ptr[iterator.pos.buffer_pos]);
			switch (states.states[1]) {
			case CSVState::INVALID:
				T::InvalidState(result);
				iterator.pos.buffer_pos++;
				bytes_read = iterator.pos.buffer_pos - start_pos;
				return;
			case CSVState::RECORD_SEPARATOR:
				if (states.states[0] == CSVState::RECORD_SEPARATOR || states.states[0] == CSVState::NOT_SET) {
					lines_read++;
					if (T::EmptyLine(result, iterator.pos.buffer_pos)) {
						iterator.pos.buffer_pos++;
						bytes_read = iterator.pos.buffer_pos - start_pos;
						return;
					}
				} else if (states.states[0] != CSVState::CARRIAGE_RETURN) {
					lines_read++;
					if (T::AddRow(result, iterator.pos.buffer_pos)) {
						iterator.pos.buffer_pos++;
						bytes_read = iterator.pos.buffer_pos - start_pos;
						return;
					}
				}
				iterator.pos.buffer_pos++;
				break;
			case CSVState::CARRIAGE_RETURN:
				lines_read++;
				if (states.states[0] == CSVState::RECORD_SEPARATOR || states.states[0] == CSVState::NOT_SET) {
					if (T::EmptyLine(result, iterator.pos.buffer_pos)) {
						iterator.pos.buffer_pos++;
						bytes_read = iterator.pos.buffer_pos - start_pos;
						return;
					}
				} else if (states.states[0] != CSVState::CARRIAGE_RETURN) {
					if (T::AddRow(result, iterator.pos.buffer_pos)) {
						iterator.pos.buffer_pos++;
						bytes_read = iterator.pos.buffer_pos - start_pos;
						return;
					}
				}
				iterator.pos.buffer_pos++;
				break;
			case CSVState::DELIMITER:
				T::AddValue(result, iterator.pos.buffer_pos);
				iterator.pos.buffer_pos++;
				break;
			case CSVState::QUOTED:
				if (states.states[0] == CSVState::UNQUOTED) {
					T::SetEscaped(result);
				}
				T::SetQuoted(result, iterator.pos.buffer_pos);
				iterator.pos.buffer_pos++;
				while (state_machine->transition_array
				           .skip_quoted[static_cast<uint8_t>(buffer_handle_ptr[iterator.pos.buffer_pos])] &&
				       iterator.pos.buffer_pos < to_pos - 1) {
					iterator.pos.buffer_pos++;
				}
				break;
			case CSVState::ESCAPE:
				T::SetEscaped(result);
				iterator.pos.buffer_pos++;
				break;
			case CSVState::STANDARD:
				iterator.pos.buffer_pos++;
				while (state_machine->transition_array
				           .skip_standard[static_cast<uint8_t>(buffer_handle_ptr[iterator.pos.buffer_pos])] &&
				       iterator.pos.buffer_pos < to_pos - 1) {
					iterator.pos.buffer_pos++;
				}
				break;
			case CSVState::QUOTED_NEW_LINE:
				T::QuotedNewLine(result);
				iterator.pos.buffer_pos++;
				break;
			default:
				iterator.pos.buffer_pos++;
				break;
			}
		}
		bytes_read = iterator.pos.buffer_pos - start_pos;
	}

	//! Finalizes the process of the chunk
	virtual void FinalizeChunkProcess();

	//! Internal function for parse chunk
	template <class T>
	void ParseChunkInternal(T &result) {
		if (!initialized) {
			Initialize();
			initialized = true;
		}
		if (!iterator.done) {
			Process(result);
		}
		FinalizeChunkProcess();
	}
};

} // namespace duckdb
