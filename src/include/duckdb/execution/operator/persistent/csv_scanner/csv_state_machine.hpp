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
#include "duckdb/execution/operator/persistent/csv_scanner/csv_state_machine_cache.hpp"

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
	explicit CSVStateMachine(CSVReaderOptions &options_p, char quote, char escape, char delim,
	                         shared_ptr<CSVBufferManager> buffer_manager_p,
	                         CSVStateMachineCache &csv_state_machine_cache_p);
	//! Resets the state machine, so it can be used again
	void Reset();

	//! Aux Function for string UTF8 Verification
	void VerifyUTF8();

	//! Prints the transition array
	void Print();

	CSVStateMachineCache &csv_state_machine_cache;

	const CSVReaderOptions &options;
	CSVBufferIterator csv_buffer_iterator;
	//! Which one was the identified start row for this file
	idx_t start_row = 0;
	//! The Transition Array is a Finite State Machine
	//! It holds the transitions of all states, on all 256 possible different characters
	state_machine_t &transition_array;

	//! Both these variables are used for new line identifier detection
	bool single_record_separator = false;
	bool carry_on_separator = false;

	//! Variables Used for Sniffing
	CSVState state;
	CSVState previous_state;
	CSVState pre_previous_state;
	idx_t cur_rows;
	idx_t column_count;
	string value;
	idx_t rows_read;

	//! Options resulting from sniffing
	char quote;
	char escape;
	char delim;
	NewLineIdentifier new_line = NewLineIdentifier::NOT_SET;
	idx_t num_cols = 0;
	bool header = false;
	std::map<LogicalTypeId, bool> has_format;
	std::map<LogicalTypeId, StrpTimeFormat> date_format;
	idx_t skip_rows = 0;
};

} // namespace duckdb
