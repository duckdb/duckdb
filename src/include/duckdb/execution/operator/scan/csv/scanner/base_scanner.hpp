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
#include "duckdb/execution/operator/scan/csv/scanner/scanner_boundary.hpp"

namespace duckdb {

class ScannerResult {
public:
	ScannerResult(CSVStates &states, CSVStateMachine &state_machine);

	idx_t Size();
	bool Empty();
	idx_t result_position = 0;

protected:
	CSVStates &states;
	CSVStateMachine &state_machine;
};

//! This is the base of our CSV scanners.
//! Scanners differ on what they are used for, and consequently have different performance benefits.
class BaseScanner {
public:
	explicit BaseScanner(shared_ptr<CSVBufferManager> buffer_manager, shared_ptr<CSVStateMachine> state_machine,
	                     CSVIterator iterator = {});

	virtual ~BaseScanner() = default;
	//! Returns true if the scanner is finished
	bool Finished();
	//! Resets the scanner
	void Reset();
	//! Parses data into a output_chunk
	virtual ScannerResult *ParseChunk();

	//! Returns the result from the last Parse call. Shouts at you if you call it wrong
	virtual ScannerResult *GetResult();

	const string &GetFileName() {
		return file_path;
	}
	const vector<string> &GetNames() {
		return names;
	}
	const vector<LogicalType> &GetTypes() {
		return types;
	}

	idx_t GetBoundaryIndex() {
		return iterator.GetBoundaryIdx();
	}

	MultiFileReaderData reader_data;
	string file_path;
	vector<string> names;
	vector<LogicalType> types;

	//! Templated function that process the parsing of a charecter
	//! OP = Operation used to alter the result of the parser
	//! T = Type of the result
	template <class T>
	inline static bool ProcessCharacter(BaseScanner &scanner, const char current_char, const idx_t buffer_pos,
	                                    T &result) {
		if (scanner.states.IsInvalid()) {
			T::Kaput(result);
			return true;
		}
		scanner.state_machine->Transition(scanner.states, current_char);
		if (scanner.states.NewValue()) {
			//! Add new value to result
			T::AddValue(result, buffer_pos);
		} else if (scanner.states.NewRow()) {
			//! Add new row to result
			//! Check if the result reached a vector size
			if (T::AddRow(result, buffer_pos)) {
				return true;
			}
		}
		//! Still have more to read
		return false;
	}

	CSVStateMachine &GetStateMachine();

	//! Produces error messages for column name -> type mismatch.
	static string ColumnTypesError(case_insensitive_map_t<idx_t> sql_types_per_column, const vector<string> &names);

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
