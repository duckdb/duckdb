#include "duckdb/execution/operator/persistent/csv_scanner/csv_state_machine.hpp"
#include "duckdb/execution/operator/persistent/csv_scanner/csv_state_machine_cache.hpp"

namespace duckdb {

void InitializeTransitionArray(unsigned char *transition_array, const uint8_t state) {
	for (uint32_t i = 0; i < NUM_TRANSITIONS; i++) {
		transition_array[i] = state;
	}
}

void CSVStateMachineCache::Insert(char delimiter, char quote, char escape) {
	D_ASSERT(state_machine_cache.find({delimiter, quote, escape}) == state_machine_cache.end());
	// Initialize transition array with default values to the Standard option
	auto &transition_array = state_machine_cache[{delimiter, quote, escape}];
	const uint8_t standard_state = static_cast<uint8_t>(CSVState::STANDARD);
	const uint8_t field_separator_state = static_cast<uint8_t>(CSVState::DELIMITER);
	const uint8_t record_separator_state = static_cast<uint8_t>(CSVState::RECORD_SEPARATOR);
	const uint8_t carriage_return_state = static_cast<uint8_t>(CSVState::CARRIAGE_RETURN);
	const uint8_t quoted_state = static_cast<uint8_t>(CSVState::QUOTED);
	const uint8_t unquoted_state = static_cast<uint8_t>(CSVState::UNQUOTED);
	const uint8_t escape_state = static_cast<uint8_t>(CSVState::ESCAPE);
	const uint8_t empty_line_state = static_cast<uint8_t>(CSVState::EMPTY_LINE);
	const uint8_t invalid_state = static_cast<uint8_t>(CSVState::INVALID);

	for (uint32_t i = 0; i < NUM_STATES; i++) {
		switch (i) {
		case quoted_state:
			InitializeTransitionArray(transition_array[i], quoted_state);
			break;
		case unquoted_state:
			InitializeTransitionArray(transition_array[i], invalid_state);
			break;
		case escape_state:
			InitializeTransitionArray(transition_array[i], invalid_state);
			break;
		default:
			InitializeTransitionArray(transition_array[i], standard_state);
			break;
		}
	}

	// Now set values depending on configuration
	// 1) Standard State
	transition_array[standard_state][static_cast<uint8_t>(delimiter)] = field_separator_state;
	transition_array[standard_state][static_cast<uint8_t>('\n')] = record_separator_state;
	transition_array[standard_state][static_cast<uint8_t>('\r')] = carriage_return_state;
	transition_array[standard_state][static_cast<uint8_t>(quote)] = quoted_state;
	// 2) Field Separator State
	transition_array[field_separator_state][static_cast<uint8_t>(delimiter)] = field_separator_state;
	transition_array[field_separator_state][static_cast<uint8_t>('\n')] = record_separator_state;
	transition_array[field_separator_state][static_cast<uint8_t>('\r')] = carriage_return_state;
	transition_array[field_separator_state][static_cast<uint8_t>(quote)] = quoted_state;
	// 3) Record Separator State
	transition_array[record_separator_state][static_cast<uint8_t>(delimiter)] = field_separator_state;
	transition_array[record_separator_state][static_cast<uint8_t>('\n')] = empty_line_state;
	transition_array[record_separator_state][static_cast<uint8_t>('\r')] = carriage_return_state;
	transition_array[record_separator_state][static_cast<uint8_t>(quote)] = quoted_state;
	// 4) Carriage Return State
	transition_array[carriage_return_state][static_cast<uint8_t>('\n')] = record_separator_state;
	transition_array[carriage_return_state][static_cast<uint8_t>('\r')] = empty_line_state;
	transition_array[carriage_return_state][static_cast<uint8_t>(escape)] = escape_state;
	// 5) Quoted State
	transition_array[quoted_state][static_cast<uint8_t>(quote)] = unquoted_state;
	if (quote != escape) {
		transition_array[quoted_state][static_cast<uint8_t>(escape)] = escape_state;
	}
	// 6) Unquoted State
	transition_array[unquoted_state][static_cast<uint8_t>('\n')] = record_separator_state;
	transition_array[unquoted_state][static_cast<uint8_t>('\r')] = carriage_return_state;
	transition_array[unquoted_state][static_cast<uint8_t>(delimiter)] = field_separator_state;
	if (quote == escape) {
		transition_array[unquoted_state][static_cast<uint8_t>(escape)] = quoted_state;
	}
	// 7) Escaped State
	transition_array[escape_state][static_cast<uint8_t>(quote)] = quoted_state;
	transition_array[escape_state][static_cast<uint8_t>(escape)] = quoted_state;
	// 8) Empty Line State
	transition_array[empty_line_state][static_cast<uint8_t>('\r')] = empty_line_state;
	transition_array[empty_line_state][static_cast<uint8_t>('\n')] = empty_line_state;
}

CSVStateMachineCache::CSVStateMachineCache() {
	for (auto quoterule : default_quote_rule) {
		const auto &quote_candidates = default_quote[static_cast<uint8_t>(quoterule)];
		for (const auto &quote : quote_candidates) {
			for (const auto &delim : default_delim) {
				const auto &escape_candidates = default_escape[static_cast<uint8_t>(quoterule)];
				for (const auto &escape : escape_candidates) {
					Insert(delim, quote, escape);
				}
			}
		}
	}
}

state_machine_t &CSVStateMachineCache::Get(char delimiter, char quote, char escape) {
	//! Custom State Machine, we need to create it and cache it first
	if (state_machine_cache.find({delimiter, quote, escape}) == state_machine_cache.end()) {
		Insert(delimiter, quote, escape);
	}
	auto &transition_array = state_machine_cache[{delimiter, quote, escape}];
	return transition_array;
}
} // namespace duckdb
