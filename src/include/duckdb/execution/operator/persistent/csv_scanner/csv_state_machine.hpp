//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/execution/operator/persistent/csv_scanner/csv_state_machine.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

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

class CSVStateMachine {
public:
	explicit CSVStateMachine(CSVStateMachineConfiguration configuration_p,
	                         shared_ptr<CSVBufferManager> buffer_manager_p);

	//! This Sniff Dialect counts how many columns are produced per row using this state machine
	void SniffDialect(vector<idx_t> &sniffed_column_counts);

	//! This sniffs values from the CSV file
	void SniffValue(vector<vector<Value>> &sniffed_value);

	//! Parses the state machine
	void Parse(vector<vector<idx_t>> &column_positions);

	//! Resets the state machine, so it can be used again
	void Reset();

	CSVStateMachineConfiguration configuration;
	CSVBufferIterator csv_buffer_iterator;

private:
	//! The Transition Array is a Finite State Machine
	//! It holds the transitions of all states, on all 256 possible different characters
	uint8_t transition_array[7][256];
	//! Current char being looked at by the machine
	char current_char;
};
} // namespace duckdb
