#include "duckdb/execution/operator/persistent/csv_scanner/csv_state_machine.hpp"
#include "duckdb/execution/operator/persistent/csv_scanner/csv_state_machine_cache.hpp"

namespace duckdb{

void CSVStateMachineCache::Insert(char delimiter, char quote, char escape) {
	// Initialize transition array with default values to the Standard option
	auto &transition_array = state_machine_cache[{delimiter, quote, escape}];
	for (uint32_t i = 0; i < 4; i++) {
		for (uint32_t j = 0; j < NUM_TRANSITIONS; j++) {
			transition_array[i][j] = static_cast<uint8_t>(CSVState::STANDARD);
		}
	}
	uint8_t standard_state = static_cast<uint8_t>(CSVState::STANDARD);
	uint8_t field_separator_state = static_cast<uint8_t>(CSVState::DELIMITER);
	uint8_t record_separator_state = static_cast<uint8_t>(CSVState::RECORD_SEPARATOR);
	uint8_t carriage_return_state = static_cast<uint8_t>(CSVState::CARRIAGE_RETURN);
	uint8_t quoted_state = static_cast<uint8_t>(CSVState::QUOTED);
	uint8_t unquoted_state = static_cast<uint8_t>(CSVState::UNQUOTED);
	uint8_t escape_state = static_cast<uint8_t>(CSVState::ESCAPE);
	uint8_t empty_line_state = static_cast<uint8_t>(CSVState::EMPTY_LINE);
	uint8_t invalid_state = static_cast<uint8_t>(CSVState::INVALID);

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
	for (uint32_t j = 0; j < NUM_TRANSITIONS; j++) {
		transition_array[quoted_state][j] = quoted_state;
	}
	transition_array[quoted_state][static_cast<uint8_t>(quote)] = unquoted_state;

	if (quote != escape) {
		transition_array[quoted_state][static_cast<uint8_t>(escape)] = escape_state;
	}
	// 6) Unquoted State
	for (uint32_t j = 0; j < NUM_TRANSITIONS; j++) {
		transition_array[unquoted_state][j] = invalid_state;
	}
	transition_array[unquoted_state][static_cast<uint8_t>('\n')] = record_separator_state;
	transition_array[unquoted_state][static_cast<uint8_t>('\r')] = carriage_return_state;
	transition_array[unquoted_state][static_cast<uint8_t>(delimiter)] = field_separator_state;
	if (quote == escape) {
		transition_array[unquoted_state][static_cast<uint8_t>(escape)] = quoted_state;
	}
	// 7) Escaped State
	for (uint32_t j = 0; j < NUM_TRANSITIONS; j++) {
		// Escape is always invalid if not proceeded by another escape or quoted char
		transition_array[escape_state][j] = invalid_state;
	}
	transition_array[escape_state][static_cast<uint8_t>(quote)] = quoted_state;
	transition_array[escape_state][static_cast<uint8_t>(escape)] = quoted_state;
	// 8) Empty Line State
	for (uint32_t j = 0; j < NUM_TRANSITIONS; j++) {
		// Escape is always invalid if not proceeded by another escape or quoted char
		transition_array[empty_line_state][j] = standard_state;
	}
	transition_array[empty_line_state][static_cast<uint8_t>('\r')] = empty_line_state;
	transition_array[empty_line_state][static_cast<uint8_t>('\n')] = empty_line_state;
}

CSVStateMachineCache::CSVStateMachineCache() {
	vector<char> default_delim = {',', '|', ';', '\t'};
	vector<vector<char>> default_quote = {{'\"'}, {'\"', '\''}, {'\0'}};
	vector<QuoteRule> default_quote_rule = {QuoteRule::QUOTES_RFC, QuoteRule::QUOTES_OTHER, QuoteRule::NO_QUOTES};
	vector<vector<char>> default_escape = {{'\0', '\"', '\''}, {'\\'}, {'\0'}};

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
}