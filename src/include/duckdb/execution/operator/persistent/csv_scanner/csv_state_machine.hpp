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
enum class CSVState {
	STANDARD = 0,         //! Regular unquoted field state
	FIELD_SEPARATOR = 1,  //! State after encountering a field separator (e.g., ;)
	RECORD_SEPARATOR = 2, //! State after encountering a record separator (e.g., \n)
	QUOTED = 3,           //! State when inside a quoted field
	ESCAPE = 4,           //! State when encountering an escape character (e.g., \)
	INVALID = 5           //! Got to an Invalid State, this should error.
};

struct CSVStateMachineConfiguration {
	string field_separator;
	string record_separator;
	string quote;
	string escape;
};

class CSVStateMachine {
public:
	explicit CSVStateMachine(CSVStateMachineConfiguration configuration_p);
	//! The Transition Array is a Finite State Machine
	//! It holds the transitions of all states, on all 256 possible different characters
	uint8_t transition_array[5][256];

private:
	CSVStateMachineConfiguration configuration;
};
} // namespace duckdb
