//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/execution/operator/persistent/csv_scanner/csv_state_machine.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "csv_buffer_manager.hpp"
#include "duckdb/execution/operator/persistent/csv_scanner/csv_reader_options.hpp"
#include "duckdb/execution/operator/persistent/csv_scanner/csv_buffer_manager.hpp"

namespace duckdb {

//! All States of CSV Parsing
enum class CSVState : uint8_t {
	STANDARD = 0,         //! Regular unquoted field state
	FIELD_SEPARATOR = 1,  //! State after encountering a field separator (e.g., ;)
	RECORD_SEPARATOR = 2, //! State after encountering a record separator (e.g., \n)
	CARRIAGE_RETURN = 3,  //! State after encountering a record separator (e.g., \n)
	QUOTED = 4,           //! State when inside a quoted field
	UNQUOTED = 5,         //! State when leaving a quoted field
	ESCAPE = 6,           //! State when encountering an escape character (e.g., \)
	INVALID = 7           //! Got to an Invalid State, this should error.
};

struct CSVStateMachineConfiguration {
	CSVStateMachineConfiguration(char field_separator_p, char quote_p, char escape_p,
	                             NewLineIdentifier record_separator_p)
	    : field_separator(field_separator_p), quote(quote_p), escape(escape_p), record_separator(record_separator_p) {
	}
	char field_separator;
	char quote;
	char escape;
	// We set the record separator through the state machine
	NewLineIdentifier record_separator;
	// Which one was the identified start row for this file
	idx_t start_row = 0;
};

//! The buffer that will be parsed by the CSV State Machine
struct StateBuffer {
	StateBuffer(char *buffer_p, idx_t buffer_size_p, idx_t position_p)
	    : buffer(buffer_p), buffer_size(buffer_size_p), position(position_p) {
	}

	StateBuffer(StateBuffer &&other) noexcept
	    : buffer(other.buffer), buffer_size(other.buffer_size), position(other.position) {
		other.buffer = nullptr;
		other.buffer_size = 0;
		other.position = 0;
	}
	//! The Buffer
	char *buffer;
	//! The Size Of The Buffer
	idx_t buffer_size;
	//! The Start Position of the buffer
	idx_t position;
};

class CSVStateMachine {
public:
	explicit CSVStateMachine(CSVStateMachineConfiguration configuration_p,
	                         shared_ptr<CSVBufferManager> buffer_manager_p);

	//! This Parse Function Finds where the lines and values start within the CSV Buffer
	//! sniff_column_count stores the number of columns for each row.
	//! start_value_idx stores the buffer idx where values start.
	//! It returns the number of rows sniffed
	void SniffDialect(vector<idx_t> &sniffed_column_counts);

	CSVStateMachineConfiguration configuration;
	CSVBufferIterator csv_buffer_iterator;

private:
	//! The Transition Array is a Finite State Machine
	//! It holds the transitions of all states, on all 256 possible different characters
	uint8_t transition_array[7][256];
};
} // namespace duckdb
