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

	//! Transition all states to next state, that depends on the current char
	inline void Transition(char current_char) {
		pre_previous_state = previous_state;
		previous_state = state;
		state = transition_array[state][static_cast<uint8_t>(current_char)];
	}

	//! Resets the state machine, so it can be used again
	void Reset();

	//! Aux Function for string UTF8 Verification
	void VerifyUTF8();

	//! The Transition Array is a Finite State Machine
	//! It holds the transitions of all states, on all 256 possible different characters
	const StateMachine &transition_array;
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
