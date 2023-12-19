//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/execution/operator/scan/csv/scanner/base_scanner.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/execution/operator/scan/csv/csv_buffer_manager.hpp"
#include "duckdb/execution/operator/scan/csv/csv_state_machine.hpp"
#include "duckdb/execution/operator/scan/csv/parser/scanner_boundary.hpp"

namespace duckdb {

struct ScannerPosition {
	//! Current  position of the buffer we are scanning
	idx_t pos = 0;
	//! Id of the buffer we are currently scanning
	idx_t buffer_id = 0;
	//! Id of the file we are currently scanning
	idx_t file_id = 0;
	//! If the position is within the scanner boundary
	bool InBoundary(const ScannerBoundary &boundary);
};

class ScannerResult {};

//! This is the base of our CSV scanners.
//! Scanners differ on what they are used for, and consequently have different performance benefits.
class BaseScanner {
public:
	explicit BaseScanner(shared_ptr<CSVBufferManager> buffer_manager, shared_ptr<CSVStateMachine> state_machine,
	                     ScannerBoundary boundary = {});

	//! Returns true if the scanner is finished
	bool Finished();
	//! Resets the scanner
	void Reset();
	//! Parses data into a output_chunk
	virtual ScannerResult *ParseChunk();

	//! Templated function that process the parsing of a charecter
	//! OP = Operation used to alter the result of the parser
	//! T = Type of the result
	template <class T>
	inline static bool ProcessCharacter(BaseScanner &scanner, const char current_char, const idx_t buffer_pos,
	                                    T &result) {
		scanner.state_machine->Transition(scanner.states, current_char);
		if (scanner.states.NewValue()) {
			//! Add new value to result
			T::AddValue(result, current_char, buffer_pos);
		} else if (scanner.states.NewRow()) {
			//! Add new row to result
			//! Check if the result reached a vector size
			T::AddRow(result, current_char, buffer_pos);
		}
		//! Still have more to read
		return false;
	}

protected:
	//! Boundaries of this scanner
	ScannerBoundary boundary;

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

	//! Holds information regarding the position we are in the csv scanner
	ScannerPosition pos;

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
