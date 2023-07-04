#include "duckdb/execution/operator/persistent/csv_scanner/csv_state_machine.hpp"
#include "duckdb/execution/operator/persistent/csv_scanner/buffered_csv_reader.hpp"

namespace duckdb {
CSVStateMachine::CSVStateMachine(CSVStateMachineConfiguration configuration_p,
                                 shared_ptr<CSVBufferManager> buffer_manager_p)
    : configuration(configuration_p), csv_buffer_iterator(std::move(buffer_manager_p)) {
	// Initialize transition array with default values to the Standard option
	for (int i = 0; i < 4; i++) {
		for (int j = 0; j < 256; j++) {
			transition_array[i][j] = static_cast<uint8_t>(CSVState::STANDARD);
		}
	}
	uint8_t standard_state = static_cast<uint8_t>(CSVState::STANDARD);
	uint8_t field_separator_state = static_cast<uint8_t>(CSVState::FIELD_SEPARATOR);
	uint8_t record_separator_state = static_cast<uint8_t>(CSVState::RECORD_SEPARATOR);
	uint8_t carriage_return_state = static_cast<uint8_t>(CSVState::CARRIAGE_RETURN);
	uint8_t quoted_state = static_cast<uint8_t>(CSVState::QUOTED);
	uint8_t unquoted_state = static_cast<uint8_t>(CSVState::UNQUOTED);
	uint8_t escape_state = static_cast<uint8_t>(CSVState::ESCAPE);
	uint8_t invalid_state = static_cast<uint8_t>(CSVState::INVALID);

	// Now set values depending on configuration
	// 1) Standard State
	transition_array[standard_state][static_cast<uint8_t>(configuration.field_separator)] = field_separator_state;
	transition_array[standard_state][static_cast<uint8_t>('\n')] = record_separator_state;
	transition_array[standard_state][static_cast<uint8_t>('\r')] = carriage_return_state;
	transition_array[standard_state][static_cast<uint8_t>(configuration.quote)] = quoted_state;
	// 2) Field Separator State
	transition_array[field_separator_state][static_cast<uint8_t>(configuration.field_separator)] =
	    field_separator_state;
	transition_array[field_separator_state][static_cast<uint8_t>('\n')] = record_separator_state;
	transition_array[field_separator_state][static_cast<uint8_t>('\r')] = carriage_return_state;
	transition_array[field_separator_state][static_cast<uint8_t>(configuration.quote)] = quoted_state;
	// 3) Record Separator State
	transition_array[record_separator_state][static_cast<uint8_t>(configuration.field_separator)] =
	    field_separator_state;
	transition_array[record_separator_state][static_cast<uint8_t>('\n')] = record_separator_state;
	transition_array[record_separator_state][static_cast<uint8_t>('\r')] = carriage_return_state;
	transition_array[record_separator_state][static_cast<uint8_t>(configuration.quote)] = quoted_state;
	// 4) Carriage Return State
	transition_array[carriage_return_state][static_cast<uint8_t>('\n')] = record_separator_state;
	transition_array[carriage_return_state][static_cast<uint8_t>('\r')] = carriage_return_state;
	transition_array[carriage_return_state][static_cast<uint8_t>(configuration.escape)] = escape_state;
	// 5) Quoted State
	for (int j = 0; j < 256; j++) {
		transition_array[quoted_state][j] = quoted_state;
	}
	transition_array[quoted_state][static_cast<uint8_t>(configuration.quote)] = unquoted_state;

	if (configuration.quote != configuration.escape) {
		transition_array[quoted_state][static_cast<uint8_t>(configuration.escape)] = escape_state;
	}
	// 6) Unquoted State
	for (int j = 0; j < 256; j++) {
		transition_array[unquoted_state][j] = invalid_state;
	}
	transition_array[unquoted_state][static_cast<uint8_t>('\n')] = record_separator_state;
	transition_array[unquoted_state][static_cast<uint8_t>('\r')] = carriage_return_state;
	transition_array[unquoted_state][static_cast<uint8_t>(configuration.field_separator)] = field_separator_state;
	if (configuration.quote == configuration.escape) {
		transition_array[unquoted_state][static_cast<uint8_t>(configuration.escape)] = quoted_state;
	}

	// 7) Escaped State
	for (int j = 0; j < 256; j++) {
		// Escape is always invalid if not proceeded by another escape or quoted char
		transition_array[escape_state][j] = invalid_state;
	}
	transition_array[escape_state][static_cast<uint8_t>(configuration.quote)] = quoted_state;
	transition_array[escape_state][static_cast<uint8_t>(configuration.escape)] = quoted_state;
	current_char = csv_buffer_iterator.GetNextChar();
}

void CSVStateMachine::SniffDialect(vector<idx_t> &sniffed_column_counts) {
	idx_t cur_rows = 0;
	idx_t column_count = 1;
	D_ASSERT(sniffed_column_counts.size() == STANDARD_VECTOR_SIZE);

	CSVState state {CSVState::STANDARD};
	CSVState previous_state;

	// Both these variables are used for new line identifier detection
	bool single_record_separator = false;
	bool carry_on_separator = false;

	while (!csv_buffer_iterator.Finished()) {
		if (state == CSVState::INVALID) {
			sniffed_column_counts.clear();
			return;
		}
		previous_state = state;

		state =
		    static_cast<CSVState>(transition_array[static_cast<uint8_t>(state)][static_cast<uint8_t>(current_char)]);
		bool empty_line = (state == CSVState::CARRIAGE_RETURN && previous_state == CSVState::CARRIAGE_RETURN) ||
		                  (state == CSVState::RECORD_SEPARATOR && previous_state == CSVState::RECORD_SEPARATOR);

		bool carriage_return = previous_state == CSVState::CARRIAGE_RETURN;
		column_count += previous_state == CSVState::FIELD_SEPARATOR;
		sniffed_column_counts[cur_rows] = column_count;
		cur_rows += previous_state == CSVState::RECORD_SEPARATOR && !empty_line;
		column_count -= (column_count - 1) * (previous_state == CSVState::RECORD_SEPARATOR);

		// It means our carriage return is actually a record separator
		cur_rows += state != CSVState::RECORD_SEPARATOR && carriage_return;
		column_count -= (column_count - 1) * (state != CSVState::RECORD_SEPARATOR && carriage_return);

		// Identify what is our line separator
		carry_on_separator = (state == CSVState::RECORD_SEPARATOR && carriage_return) || carry_on_separator;
		single_record_separator = ((state != CSVState::RECORD_SEPARATOR && carriage_return) ||
		                           (state == CSVState::RECORD_SEPARATOR && !carriage_return)) ||
		                          single_record_separator;
		if (cur_rows >= STANDARD_VECTOR_SIZE) {
			// We sniffed enough rows
			break;
		}
		current_char = csv_buffer_iterator.GetNextChar();
	}
	bool empty_line = (state == CSVState::CARRIAGE_RETURN && previous_state == CSVState::CARRIAGE_RETURN) ||
	                  (state == CSVState::RECORD_SEPARATOR && previous_state == CSVState::RECORD_SEPARATOR);
	if (cur_rows < STANDARD_VECTOR_SIZE && !empty_line) {
		sniffed_column_counts[cur_rows++] = column_count;
	}
	NewLineIdentifier suggested_newline;
	if (carry_on_separator) {
		if (single_record_separator) {
			suggested_newline = NewLineIdentifier::MIX;
		} else {
			suggested_newline = NewLineIdentifier::CARRY_ON;
		}
	} else {
		suggested_newline = NewLineIdentifier::SINGLE;
	}
	if (configuration.record_separator == NewLineIdentifier::NOT_SET) {
		configuration.record_separator = suggested_newline;
	} else {
		if (configuration.record_separator != suggested_newline) {
			// Invalidate this whole detection
			cur_rows = 0;
		}
	}
	sniffed_column_counts.erase(sniffed_column_counts.end() - (STANDARD_VECTOR_SIZE - cur_rows),
	                            sniffed_column_counts.end());
}

} // namespace duckdb
