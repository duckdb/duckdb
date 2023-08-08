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



class CSVStateMachine {
public:
	explicit CSVStateMachine(CSVReaderOptions options_p, shared_ptr<CSVBufferManager> buffer_manager_p);

	//! This sniffs values from the CSV file
	void SniffValue(vector<pair<idx_t, vector<Value>>> &sniffed_value);

	//! Parses the state machine
	void Parse(DataChunk &parse_chunk);

	//! Resets the state machine, so it can be used again
	void Reset();
	CSVReaderOptions options;
	CSVBufferIterator csv_buffer_iterator;
	//! Which one was the identified start row for this file
	idx_t start_row = 0;
	//! The Transition Array is a Finite State Machine
	//! It holds the transitions of all states, on all 256 possible different characters
	uint8_t transition_array[7][256];

	// Both these variables are used for new line identifier detection
	bool single_record_separator = false;
	bool carry_on_separator = false;
	CSVState state {CSVState::STANDARD};
	CSVState previous_state {CSVState::STANDARD};
	CSVState pre_previous_state {CSVState::STANDARD};
	idx_t cur_rows = 0;
	idx_t column_count = 1;
private:

	//! Current char being looked at by the machine
	char current_char;
};

struct SniffDialect{
	inline static bool Process(CSVStateMachine& machine,vector<idx_t> &sniffed_column_counts, char current_char){

	D_ASSERT(sniffed_column_counts.size() == machine.options.sample_chunk_size);

	if (machine.state == CSVState::INVALID) {
		sniffed_column_counts.clear();
		return true;
	}
	machine.pre_previous_state = machine.previous_state;
	machine.previous_state = machine.state;

	machine.state =
	    static_cast<CSVState>(machine.transition_array[static_cast<uint8_t>(machine.state)][static_cast<uint8_t>(current_char)]);
	bool empty_line =
	    (machine.state == CSVState::CARRIAGE_RETURN && machine.previous_state == CSVState::CARRIAGE_RETURN) ||
	    (machine.state == CSVState::RECORD_SEPARATOR && machine.previous_state == CSVState::RECORD_SEPARATOR) ||
	    (machine.state == CSVState::CARRIAGE_RETURN && machine.previous_state == CSVState::RECORD_SEPARATOR) ||
	    (machine.pre_previous_state == CSVState::RECORD_SEPARATOR && machine.previous_state == CSVState::CARRIAGE_RETURN);

	bool carriage_return = machine.previous_state == CSVState::CARRIAGE_RETURN;
	machine.column_count += machine.previous_state == CSVState::FIELD_SEPARATOR;
	sniffed_column_counts[machine.cur_rows] = machine.column_count;
	machine.cur_rows += machine.previous_state == CSVState::RECORD_SEPARATOR && !empty_line;
	machine.column_count -= (machine.column_count - 1) * (machine.previous_state == CSVState::RECORD_SEPARATOR);

	// It means our carriage return is actually a record separator
	machine.cur_rows += machine.state != CSVState::RECORD_SEPARATOR && carriage_return;
	machine.column_count -= (machine.column_count - 1) * (machine.state != CSVState::RECORD_SEPARATOR && carriage_return);

	// Identify what is our line separator
	machine.carry_on_separator = (machine.state == CSVState::RECORD_SEPARATOR && carriage_return) || machine.carry_on_separator;
	machine.single_record_separator = ((machine.state != CSVState::RECORD_SEPARATOR && carriage_return) ||
	                           (machine.state == CSVState::RECORD_SEPARATOR && !carriage_return)) ||
	                          machine.single_record_separator;
	if (machine.cur_rows >= machine.options.sample_chunk_size) {
		// We sniffed enough rows
		return true;
	}
	return false;
}
	inline static void Finalize(CSVStateMachine& machine,vector<idx_t> &sniffed_column_counts){
	if (machine.state == CSVState::INVALID){
		return;
	}
	bool empty_line = (machine.state == CSVState::CARRIAGE_RETURN && machine.previous_state == CSVState::CARRIAGE_RETURN) ||
	                  (machine.state == CSVState::RECORD_SEPARATOR && machine.previous_state == CSVState::RECORD_SEPARATOR) ||
	                  (machine.state == CSVState::CARRIAGE_RETURN && machine.previous_state == CSVState::RECORD_SEPARATOR) ||
	                  (machine.pre_previous_state == CSVState::RECORD_SEPARATOR && machine.previous_state == CSVState::CARRIAGE_RETURN);
	if (machine.cur_rows < machine.options.sample_chunk_size && !empty_line) {
		sniffed_column_counts[machine.cur_rows++] = machine.column_count;
	}
	NewLineIdentifier suggested_newline;
	if (machine.carry_on_separator) {
		if (machine.single_record_separator) {
			suggested_newline = NewLineIdentifier::MIX;
		} else {
			suggested_newline = NewLineIdentifier::CARRY_ON;
		}
	} else {
		suggested_newline = NewLineIdentifier::SINGLE;
	}
	if (machine.options.new_line == NewLineIdentifier::NOT_SET) {
		machine.options.new_line = suggested_newline;
	} else {
		if (machine.options.new_line != suggested_newline) {
			// Invalidate this whole detection
			machine.cur_rows = 0;
		}
	}
	sniffed_column_counts.erase(sniffed_column_counts.end() - (machine.options.sample_chunk_size - machine.cur_rows),
	                            sniffed_column_counts.end());

}

};
} // namespace duckdb
