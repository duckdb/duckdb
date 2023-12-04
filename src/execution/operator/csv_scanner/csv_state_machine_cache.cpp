#include "duckdb/execution/operator/scan/csv/csv_state_machine.hpp"
#include "duckdb/execution/operator/scan/csv/csv_state_machine_cache.hpp"

namespace duckdb {

void InitializeTransitionArray(CSVState *transition_array, const CSVState state) {
	for (uint32_t i = 0; i < StateMachine::NUM_TRANSITIONS; i++) {
		transition_array[i] = state;
	}
}

void CSVStateMachineCache::Insert(const CSVStateMachineOptions &state_machine_options) {
	D_ASSERT(state_machine_cache.find(state_machine_options) == state_machine_cache.end());
	// Initialize transition array with default values to the Standard option
	auto &transition_array = state_machine_cache[state_machine_options];

	for (uint32_t i = 0; i < StateMachine::NUM_STATES; i++) {
		CSVState cur_state = CSVState(i);
		switch (cur_state) {
		case CSVState::QUOTED:
			InitializeTransitionArray(transition_array[cur_state], CSVState::QUOTED);
			break;
		case CSVState::UNQUOTED:
		case CSVState::INVALID:
		case CSVState::ESCAPE:
			InitializeTransitionArray(transition_array[cur_state], CSVState::INVALID);
			break;
		default:
			InitializeTransitionArray(transition_array[cur_state], CSVState::STANDARD);
			break;
		}
	}

	// Now set values depending on configuration
	// 1) Standard State
	transition_array[CSVState::STANDARD][static_cast<uint8_t>(state_machine_options.delimiter)] = CSVState::DELIMITER;
	transition_array[CSVState::STANDARD][static_cast<uint8_t>('\n')] = CSVState::RECORD_SEPARATOR;
	transition_array[CSVState::STANDARD][static_cast<uint8_t>('\r')] = CSVState::CARRIAGE_RETURN;
	transition_array[CSVState::STANDARD][static_cast<uint8_t>(state_machine_options.quote)] = CSVState::QUOTED;
	// 2) Field Separator State
	transition_array[CSVState::DELIMITER][static_cast<uint8_t>(state_machine_options.delimiter)] = CSVState::DELIMITER;
	transition_array[CSVState::DELIMITER][static_cast<uint8_t>('\n')] = CSVState::RECORD_SEPARATOR;
	transition_array[CSVState::DELIMITER][static_cast<uint8_t>('\r')] = CSVState::CARRIAGE_RETURN;
	transition_array[CSVState::DELIMITER][static_cast<uint8_t>(state_machine_options.quote)] = CSVState::QUOTED;
	// 3) Record Separator State
	transition_array[CSVState::RECORD_SEPARATOR][static_cast<uint8_t>(state_machine_options.delimiter)] =
	    CSVState::DELIMITER;
	transition_array[CSVState::RECORD_SEPARATOR][static_cast<uint8_t>('\n')] = CSVState::EMPTY_LINE;
	transition_array[CSVState::RECORD_SEPARATOR][static_cast<uint8_t>('\r')] = CSVState::EMPTY_LINE;
	transition_array[CSVState::RECORD_SEPARATOR][static_cast<uint8_t>(state_machine_options.quote)] = CSVState::QUOTED;
	// 4) Carriage Return State
	transition_array[CSVState::CARRIAGE_RETURN][static_cast<uint8_t>('\n')] = CSVState::RECORD_SEPARATOR;
	transition_array[CSVState::CARRIAGE_RETURN][static_cast<uint8_t>('\r')] = CSVState::EMPTY_LINE;
	transition_array[CSVState::CARRIAGE_RETURN][static_cast<uint8_t>(state_machine_options.escape)] = CSVState::ESCAPE;
	// 5) Quoted State
	transition_array[CSVState::QUOTED][static_cast<uint8_t>(state_machine_options.quote)] = CSVState::UNQUOTED;
	if (state_machine_options.quote != state_machine_options.escape) {
		transition_array[CSVState::QUOTED][static_cast<uint8_t>(state_machine_options.escape)] = CSVState::ESCAPE;
	}
	// 6) Unquoted State
	transition_array[CSVState::UNQUOTED][static_cast<uint8_t>('\n')] = CSVState::RECORD_SEPARATOR;
	transition_array[CSVState::UNQUOTED][static_cast<uint8_t>('\r')] = CSVState::CARRIAGE_RETURN;
	transition_array[CSVState::UNQUOTED][static_cast<uint8_t>(state_machine_options.delimiter)] = CSVState::DELIMITER;
	if (state_machine_options.quote == state_machine_options.escape) {
		transition_array[CSVState::UNQUOTED][static_cast<uint8_t>(state_machine_options.escape)] = CSVState::QUOTED;
	}
	// 7) Escaped State
	transition_array[CSVState::ESCAPE][static_cast<uint8_t>(state_machine_options.quote)] = CSVState::QUOTED;
	transition_array[CSVState::ESCAPE][static_cast<uint8_t>(state_machine_options.escape)] = CSVState::QUOTED;
	// 8) Empty Line State
	transition_array[CSVState::EMPTY_LINE][static_cast<uint8_t>('\r')] = CSVState::EMPTY_LINE;
	transition_array[CSVState::EMPTY_LINE][static_cast<uint8_t>('\n')] = CSVState::EMPTY_LINE;
	transition_array[CSVState::EMPTY_LINE][static_cast<uint8_t>(state_machine_options.delimiter)] = CSVState::DELIMITER;
	transition_array[CSVState::EMPTY_LINE][static_cast<uint8_t>(state_machine_options.quote)] = CSVState::QUOTED;
}

CSVStateMachineCache::CSVStateMachineCache() {
	for (auto quoterule : default_quote_rule) {
		const auto &quote_candidates = default_quote[static_cast<uint8_t>(quoterule)];
		for (const auto &quote : quote_candidates) {
			for (const auto &delimiter : default_delimiter) {
				const auto &escape_candidates = default_escape[static_cast<uint8_t>(quoterule)];
				for (const auto &escape : escape_candidates) {
					Insert({delimiter, quote, escape});
				}
			}
		}
	}
}

const StateMachine &CSVStateMachineCache::Get(const CSVStateMachineOptions &state_machine_options) {
	//! Custom State Machine, we need to create it and cache it first
	if (state_machine_cache.find(state_machine_options) == state_machine_cache.end()) {
		Insert(state_machine_options);
	}
	const auto &transition_array = state_machine_cache[state_machine_options];
	return transition_array;
}
} // namespace duckdb
