//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/execution/operator/scan/csv/csv_state_machine.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/execution/operator/scan/csv/csv_reader_options.hpp"
#include "duckdb/execution/operator/scan/csv/csv_buffer_manager.hpp"
#include "duckdb/execution/operator/scan/csv/csv_state_machine_cache.hpp"

namespace duckdb {

//! All States of CSV Parsing
enum class CSVState : uint8_t {
	STANDARD = 0,         //! Regular unquoted field state
	DELIMITER = 1,        //! State after encountering a field separator (e.g., ;)
	RECORD_SEPARATOR = 2, //! State after encountering a record separator (i.e., \n)
	CARRIAGE_RETURN = 3,  //! State after encountering a carriage return(i.e., \r)
	QUOTED = 4,           //! State when inside a quoted field
	UNQUOTED = 5,         //! State when leaving a quoted field
	ESCAPE = 6,           //! State when encountering an escape character (e.g., \)
	EMPTY_LINE = 7,       //! State when encountering an empty line (i.e., \r\r \n\n, \n\r)
	INVALID = 8           //! Got to an Invalid State, this should error.
};

//! The CSV State Machine comprises a state transition array (STA).
//! The STA indicates the current state of parsing based on both the current and preceding characters.
//! This reveals whether we are dealing with a Field, a New Line, a Delimiter, and so forth.
//! The STA's creation depends on the provided quote, character, and delimiter options for that state machine.
//! The motivation behind implementing an STA is to remove branching in regular CSV Parsing by predicting and detecting
//! the states. Note: The State Machine is currently utilized solely in the CSV Sniffer.
class CSVStateMachine {
public:
	explicit CSVStateMachine(CSVReaderOptions &options_p, const CSVStateMachineOptions &state_machine_options,
	                         CSVStateMachineCache &csv_state_machine_cache_p);

	//! The Transition Array is a Finite State Machine
	//! It holds the transitions of all states, on all 256 possible different characters
	const state_machine_t &transition_array;
	//! Options of this state machine
	const CSVStateMachineOptions state_machine_options;
	//! CSV Reader Options
	const CSVReaderOptions &options;
};

//! State Machine holding options that are detected during sniffing
class CSVStateMachineSniffing : public CSVStateMachine {
public:
	explicit CSVStateMachineSniffing(CSVReaderOptions &options_p, const CSVStateMachineOptions &state_machine_options,
	                                 CSVStateMachineCache &csv_state_machine_cache_p);
	//! Stores identified start row for this file (e.g., a file can start with garbage like notes, before the header)
	idx_t start_row = 0;

	//! Both these variables are used for new line identifier detection
	bool single_record_separator = false;
	bool carry_on_separator = false;
	//! Dialect options resulting from sniffing
	DialectOptions dialect_options;
};

} // namespace duckdb
