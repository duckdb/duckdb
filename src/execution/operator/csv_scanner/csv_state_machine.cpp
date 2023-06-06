#include "duckdb/execution/operator/persistent/csv_scanner/csv_state_machine.hpp"

namespace duckdb {
CSVStateMachine::CSVStateMachine(CSVStateMachineConfiguration configuration_p) : configuration(configuration_p) {
	// Initialize transition array with default values to the Standard option

	for (int i = 0; i < 5; i++) {
		for (int j = 0; j < 256; j++) {
			transition_array[i][j] = static_cast<uint8_t>(CSVState::STANDARD);
		}
	}
	uint8_t standard_state = static_cast<uint8_t>(CSVState::STANDARD);
	uint8_t field_separator_state = static_cast<uint8_t>(CSVState::FIELD_SEPARATOR);
	uint8_t record_separator_state = static_cast<uint8_t>(CSVState::RECORD_SEPARATOR);
	uint8_t quoted_state = static_cast<uint8_t>(CSVState::QUOTED);
	uint8_t escape_state = static_cast<uint8_t>(CSVState::ESCAPE);
	uint8_t invalid_state = static_cast<uint8_t>(CSVState::INVALID);
	// FIXME: For now let's only care about single-char options
	// Now set values depending on configuration
	// 1) Standard State
	for (int j = 0; j < 256; j++) {
		transition_array[standard_state][j] = static_cast<uint8_t>(CSVState::STANDARD);
	}
	transition_array[standard_state][static_cast<uint8_t>(configuration.field_separator[0])] = field_separator_state;
	transition_array[standard_state][static_cast<uint8_t>(configuration.record_separator[0])] = record_separator_state;
	transition_array[standard_state][static_cast<uint8_t>(configuration.quote[0])] = quoted_state;
	// 2) Field Separator State
	for (int j = 0; j < 256; j++) {
		transition_array[field_separator_state][j] = standard_state;
	}
	transition_array[field_separator_state][static_cast<uint8_t>(configuration.field_separator[0])] =
	    field_separator_state;
	transition_array[field_separator_state][static_cast<uint8_t>(configuration.record_separator[0])] =
	    record_separator_state;
	transition_array[field_separator_state][static_cast<uint8_t>(configuration.quote[0])] = quoted_state;
	// 3) Record Separator State
	// FIXME: Carriage return state?
	for (int j = 0; j < 256; j++) {
		transition_array[record_separator_state][j] = standard_state;
	}
	transition_array[record_separator_state][static_cast<uint8_t>(configuration.field_separator[0])] =
	    field_separator_state;
	transition_array[record_separator_state][static_cast<uint8_t>(configuration.record_separator[0])] =
	    record_separator_state;
	transition_array[record_separator_state][static_cast<uint8_t>(configuration.quote[0])] = quoted_state;

	// 4) Quoted State
	for (int j = 0; j < 256; j++) {
		transition_array[quoted_state][j] = quoted_state;
	}
	transition_array[quoted_state][static_cast<uint8_t>(configuration.quote[0])] = standard_state;
	transition_array[quoted_state][static_cast<uint8_t>(configuration.escape[0])] = escape_state;

	// 5) Escaped State
	for (int j = 0; j < 256; j++) {
		transition_array[escape_state][j] = invalid_state;
	}
	transition_array[escape_state][static_cast<uint8_t>(configuration.quote[0])] = quoted_state;
	transition_array[escape_state][static_cast<uint8_t>(configuration.escape[0])] = quoted_state;
}

void CSVStateMachine::SniffColumns(StateBuffer &buffer, vector<idx_t> &sniffed_column_counts, idx_t max_rows) {
	idx_t cur_rows = 0;
	idx_t cur_pos = buffer.position;
	idx_t column_count = 0;
	idx_t record_idx = 0;
	// FIXME: Guessing there are not more than 100 columns, should probably do a check on the first line or sth
	if (sniffed_column_counts.size() != STANDARD_VECTOR_SIZE) {
		sniffed_column_counts.resize(STANDARD_VECTOR_SIZE);
	}
	CSVState state {CSVState::STANDARD};
	while (cur_pos < buffer.buffer_size && cur_rows < max_rows) {
		auto c = (buffer.buffer.get())[cur_pos];
		column_count += state == CSVState::FIELD_SEPARATOR;
		sniffed_column_counts[record_idx] = column_count;
		record_idx += state == CSVState::RECORD_SEPARATOR;
		column_count -= column_count * (state == CSVState::RECORD_SEPARATOR);
		state = static_cast<CSVState>(transition_array[static_cast<idx_t>(state)][static_cast<idx_t>(c)]);
		cur_pos++;
	}
}

} // namespace duckdb
