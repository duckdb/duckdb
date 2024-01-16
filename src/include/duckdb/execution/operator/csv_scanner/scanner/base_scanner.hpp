//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/execution/operator/csv_scanner/scanner/base_scanner.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/execution/operator/csv_scanner/buffer_manager/csv_buffer_manager.hpp"
#include "duckdb/execution/operator/csv_scanner/scanner/scanner_boundary.hpp"
#include "duckdb/execution/operator/csv_scanner/state_machine/csv_state_machine.hpp"
#include "duckdb/execution/operator/csv_scanner/util/csv_error.hpp"

namespace duckdb {

class CSVFileScan;
class ScannerResult {
public:
	ScannerResult(CSVStates &states, CSVStateMachine &state_machine);

	idx_t Size();
	bool Empty();
	idx_t result_position = 0;

	//! Adds a Value to the result
	static inline void SetQuoted(ScannerResult &result) {
		result.quoted = true;
	}
	//! Adds a Row to the result
	static inline void SetEscaped(ScannerResult &result) {
		result.escaped = true;
	}
	// Variable to keep information regarding quoted and escaped values
	bool quoted = false;
	bool escaped = false;

protected:
	CSVStates &states;
	CSVStateMachine &state_machine;
};

//! This is the base of our CSV scanners.
//! Scanners differ on what they are used for, and consequently have different performance benefits.
class BaseScanner {
public:
	explicit BaseScanner(shared_ptr<CSVBufferManager> buffer_manager, shared_ptr<CSVStateMachine> state_machine,
	                     shared_ptr<CSVErrorHandler> error_handler, CSVIterator iterator = {});

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

	//! Templated function that process the parsing of a character
	//! OP = Operation used to alter the result of the parser
	//! T = Type of the result
	template <class T>
	inline static bool ProcessCharacter(BaseScanner &scanner, const char current_char, const idx_t buffer_pos,
	                                    T &result) {
		scanner.state_machine->Transition(scanner.states, current_char);
		switch (scanner.states.current_state) {
		case CSVState::INVALID:
			T::InvalidState(result);
			return true;
		case CSVState::RECORD_SEPARATOR:
			if (scanner.states.previous_state == CSVState::RECORD_SEPARATOR ||
			    (scanner.states.previous_state == CSVState::CARRIAGE_RETURN &&
			     scanner.states.pre_previous_state == CSVState::RECORD_SEPARATOR)) {
				scanner.lines_read++;
				return T::EmptyLine(result, buffer_pos);
			} else if (scanner.states.previous_state != CSVState::CARRIAGE_RETURN) {
				scanner.lines_read++;
				return T::AddRow(result, buffer_pos);
			}
			return false;
		case CSVState::CARRIAGE_RETURN:
			scanner.lines_read++;
			if (scanner.states.previous_state != CSVState::RECORD_SEPARATOR) {
				return T::AddRow(result, buffer_pos);
			}
			return false;
		case CSVState::DELIMITER:
			T::AddValue(result, buffer_pos);
			return false;
		case CSVState::QUOTED:
			if (scanner.states.previous_state == CSVState::UNQUOTED) {
				T::SetEscaped(result);
			}
			T::SetQuoted(result);
			return false;
		case CSVState::ESCAPE:
			T::SetEscaped(result);
			return false;
		default:
			return false;
		}
	}

	CSVStateMachine &GetStateMachine();

	shared_ptr<CSVFileScan> csv_file_scan;

	//! If this scanner is being used for sniffing
	bool sniffing = false;
	//! The guy that handles errors
	shared_ptr<CSVErrorHandler> error_handler;

protected:
	//! Boundaries of this scanner
	CSVIterator iterator;

	//! Unique pointer to the buffer_handle, this is unique per scanner, since it also contains the necessary counters
	//! To offload buffers to disk if necessary
	unique_ptr<CSVBufferHandle> cur_buffer_handle;

	//! Hold the current buffer ptr
	char *buffer_handle_ptr = nullptr;

	//! Shared pointer to the buffer_manager, this is shared across multiple scanners
	shared_ptr<CSVBufferManager> buffer_manager;

	//! Shared pointer to the state machine, this is used across multiple scanners
	shared_ptr<CSVStateMachine> state_machine;
	//! If this scanner has been initialized
	bool initialized = false;
	//! How many lines were read by this scanner
	idx_t lines_read = 0;
	//! States
	CSVStates states;

	//! Internal Functions used to perform the parsing
	//! Initializes the scanner
	virtual void Initialize();

	//! Process one chunk
	virtual void Process();

	//! Finalizes the process of the chunk
	virtual void FinalizeChunkProcess();

	//! Internal function for parse chunk
	void ParseChunkInternal();
};

} // namespace duckdb
