//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/execution/operator/persistent/csv_scanner/csv_state_machine.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

namespace duckdb {

//! All States of CSV Parsing
enum class CSVState : uint8_t {
	STANDARD = 0,         //! Regular unquoted field state
	FIELD_SEPARATOR = 1,  //! State after encountering a field separator (e.g., ;)
	RECORD_SEPARATOR = 2, //! State after encountering a record separator (e.g., \n)
	QUOTED = 3,           //! State when inside a quoted field
	ESCAPE = 4,           //! State when encountering an escape character (e.g., \)
	INVALID = 5           //! Got to an Invalid State, this should error.
};

struct CSVStateMachineConfiguration {
	CSVStateMachineConfiguration(string field_separator_p, string record_separator_p, string quote_p, string escape_p)
	    : field_separator(field_separator_p), record_separator(record_separator_p), quote(quote_p), escape(escape_p) {
	}
	string field_separator;
	string record_separator;
	string quote;
	string escape;
};

//! The buffer that will be parsed by the CSV State Machine
struct StateBuffer {
	StateBuffer(char *buffer_p, idx_t buffer_size_p, idx_t position_p)
	    : buffer(buffer_p), buffer_size(buffer_size_p), position(position_p) {
	}
	//! The Buffer
	unsafe_unique_array<char> buffer;
	//! The Size Of The Buffer
	idx_t buffer_size;
	//! The Start Position of the buffer
	idx_t position;
};

class CSVStateMachine {
public:
	explicit CSVStateMachine(CSVStateMachineConfiguration configuration_p);

	//! This Parse Function Finds where the lines and values start within the CSV Buffer
	//! start_row_idx stores the buffer idx where rows start.
	//! start_value_idx stores the buffer idx where values start.
	//! max_rows is the maximum number of rows to be parsed by this function.
	void SniffColumns(StateBuffer &buffer, vector<idx_t> &sniff_column_count, idx_t max_rows);

	const CSVStateMachineConfiguration configuration;

private:
	//! The Transition Array is a Finite State Machine
	//! It holds the transitions of all states, on all 256 possible different characters
	uint8_t transition_array[5][256];
};
} // namespace duckdb
