#include "duckdb/execution/operator/csv_scanner/csv_state_machine.hpp"
#include "duckdb/execution/operator/csv_scanner/csv_state_machine_cache.hpp"

namespace duckdb {

void InitializeTransitionArray(StateMachine &transition_array, const CSVState cur_state, const CSVState state) {
	for (uint32_t i = 0; i < StateMachine::NUM_TRANSITIONS; i++) {
		transition_array[i][static_cast<uint8_t>(cur_state)] = state;
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
		case CSVState::QUOTED_NEW_LINE:
			InitializeTransitionArray(transition_array, cur_state, CSVState::QUOTED);
			break;
		case CSVState::UNQUOTED:
		case CSVState::ESCAPE:
			InitializeTransitionArray(transition_array, cur_state, CSVState::INVALID);
			break;
		default:
			InitializeTransitionArray(transition_array, cur_state, CSVState::STANDARD);
			break;
		}
	}

	uint8_t delimiter = static_cast<uint8_t>(state_machine_options.delimiter.GetValue());
	uint8_t quote = static_cast<uint8_t>(state_machine_options.quote.GetValue());
	uint8_t escape = static_cast<uint8_t>(state_machine_options.escape.GetValue());

	auto new_line_id = state_machine_options.new_line.GetValue();

	// Now set values depending on configuration
	// 1) Standard/Invalid State
	vector<uint8_t> std_inv {static_cast<uint8_t>(CSVState::STANDARD), static_cast<uint8_t>(CSVState::INVALID)};
	for (auto &state : std_inv) {
		transition_array[delimiter][state] = CSVState::DELIMITER;
		transition_array[static_cast<uint8_t>('\n')][state] = CSVState::RECORD_SEPARATOR;
		if (new_line_id == NewLineIdentifier::CARRY_ON) {
			transition_array[static_cast<uint8_t>('\r')][state] = CSVState::CARRIAGE_RETURN;
		} else {
			transition_array[static_cast<uint8_t>('\r')][state] = CSVState::RECORD_SEPARATOR;
		}
	}
	// 2) Field Separator State
	transition_array[delimiter][static_cast<uint8_t>(CSVState::DELIMITER)] = CSVState::DELIMITER;
	transition_array[static_cast<uint8_t>('\n')][static_cast<uint8_t>(CSVState::DELIMITER)] =
	    CSVState::RECORD_SEPARATOR;
	if (new_line_id == NewLineIdentifier::CARRY_ON) {
		transition_array[static_cast<uint8_t>('\r')][static_cast<uint8_t>(CSVState::DELIMITER)] =
		    CSVState::CARRIAGE_RETURN;
	} else {
		transition_array[static_cast<uint8_t>('\r')][static_cast<uint8_t>(CSVState::DELIMITER)] =
		    CSVState::RECORD_SEPARATOR;
	}
	transition_array[quote][static_cast<uint8_t>(CSVState::DELIMITER)] = CSVState::QUOTED;
	if (delimiter != ' ') {
		transition_array[' '][static_cast<uint8_t>(CSVState::DELIMITER)] = CSVState::EMPTY_SPACE;
	}

	// 3) Record Separator State
	transition_array[delimiter][static_cast<uint8_t>(CSVState::RECORD_SEPARATOR)] = CSVState::DELIMITER;
	transition_array[static_cast<uint8_t>('\n')][static_cast<uint8_t>(CSVState::RECORD_SEPARATOR)] =
	    CSVState::RECORD_SEPARATOR;
	if (new_line_id == NewLineIdentifier::CARRY_ON) {
		transition_array[static_cast<uint8_t>('\r')][static_cast<uint8_t>(CSVState::RECORD_SEPARATOR)] =
		    CSVState::CARRIAGE_RETURN;
	} else {
		transition_array[static_cast<uint8_t>('\r')][static_cast<uint8_t>(CSVState::RECORD_SEPARATOR)] =
		    CSVState::RECORD_SEPARATOR;
	}
	transition_array[quote][static_cast<uint8_t>(CSVState::RECORD_SEPARATOR)] = CSVState::QUOTED;
	if (delimiter != ' ') {
		transition_array[' '][static_cast<uint8_t>(CSVState::RECORD_SEPARATOR)] = CSVState::EMPTY_SPACE;
	}

	// 4) Carriage Return State
	transition_array[static_cast<uint8_t>('\n')][static_cast<uint8_t>(CSVState::CARRIAGE_RETURN)] =
	    CSVState::RECORD_SEPARATOR;
	transition_array[static_cast<uint8_t>('\r')][static_cast<uint8_t>(CSVState::CARRIAGE_RETURN)] =
	    CSVState::CARRIAGE_RETURN;
	transition_array[quote][static_cast<uint8_t>(CSVState::CARRIAGE_RETURN)] = CSVState::QUOTED;
	if (delimiter != ' ') {
		transition_array[' '][static_cast<uint8_t>(CSVState::CARRIAGE_RETURN)] = CSVState::EMPTY_SPACE;
	}

	// 5) Quoted State
	transition_array[quote][static_cast<uint8_t>(CSVState::QUOTED)] = CSVState::UNQUOTED;
	transition_array['\n'][static_cast<uint8_t>(CSVState::QUOTED)] = CSVState::QUOTED_NEW_LINE;
	transition_array['\r'][static_cast<uint8_t>(CSVState::QUOTED)] = CSVState::QUOTED_NEW_LINE;

	if (state_machine_options.quote != state_machine_options.escape) {
		transition_array[escape][static_cast<uint8_t>(CSVState::QUOTED)] = CSVState::ESCAPE;
	}
	// 6) Unquoted State
	transition_array[static_cast<uint8_t>('\n')][static_cast<uint8_t>(CSVState::UNQUOTED)] = CSVState::RECORD_SEPARATOR;
	if (new_line_id == NewLineIdentifier::CARRY_ON) {
		transition_array[static_cast<uint8_t>('\r')][static_cast<uint8_t>(CSVState::UNQUOTED)] =
		    CSVState::CARRIAGE_RETURN;
	} else {
		transition_array[static_cast<uint8_t>('\r')][static_cast<uint8_t>(CSVState::UNQUOTED)] =
		    CSVState::RECORD_SEPARATOR;
	}
	transition_array[delimiter][static_cast<uint8_t>(CSVState::UNQUOTED)] = CSVState::DELIMITER;
	if (state_machine_options.quote == state_machine_options.escape) {
		transition_array[escape][static_cast<uint8_t>(CSVState::UNQUOTED)] = CSVState::QUOTED;
	}
	// 7) Escaped State
	transition_array[quote][static_cast<uint8_t>(CSVState::ESCAPE)] = CSVState::QUOTED;
	transition_array[escape][static_cast<uint8_t>(CSVState::ESCAPE)] = CSVState::QUOTED;

	// 8) Not Set
	transition_array[delimiter][static_cast<uint8_t>(static_cast<uint8_t>(CSVState::NOT_SET))] = CSVState::DELIMITER;
	transition_array[static_cast<uint8_t>('\n')][static_cast<uint8_t>(CSVState::NOT_SET)] = CSVState::RECORD_SEPARATOR;
	if (new_line_id == NewLineIdentifier::CARRY_ON) {
		transition_array[static_cast<uint8_t>('\r')][static_cast<uint8_t>(CSVState::NOT_SET)] =
		    CSVState::CARRIAGE_RETURN;
	} else {
		transition_array[static_cast<uint8_t>('\r')][static_cast<uint8_t>(CSVState::NOT_SET)] =
		    CSVState::RECORD_SEPARATOR;
	}
	transition_array[static_cast<uint8_t>(quote)][static_cast<uint8_t>(CSVState::NOT_SET)] = CSVState::QUOTED;
	if (delimiter != ' ') {
		transition_array[' '][static_cast<uint8_t>(CSVState::NOT_SET)] = CSVState::EMPTY_SPACE;
	}
	// 9) Quoted NewLine
	transition_array[quote][static_cast<uint8_t>(CSVState::QUOTED_NEW_LINE)] = CSVState::UNQUOTED;
	if (state_machine_options.quote != state_machine_options.escape) {
		transition_array[escape][static_cast<uint8_t>(CSVState::QUOTED_NEW_LINE)] = CSVState::ESCAPE;
	}

	// 10) Empty Value State
	transition_array[delimiter][static_cast<uint8_t>(static_cast<uint8_t>(CSVState::EMPTY_SPACE))] =
	    CSVState::DELIMITER;
	transition_array[static_cast<uint8_t>('\n')][static_cast<uint8_t>(CSVState::EMPTY_SPACE)] =
	    CSVState::RECORD_SEPARATOR;
	if (new_line_id == NewLineIdentifier::CARRY_ON) {
		transition_array[static_cast<uint8_t>('\r')][static_cast<uint8_t>(CSVState::EMPTY_SPACE)] =
		    CSVState::CARRIAGE_RETURN;
	} else {
		transition_array[static_cast<uint8_t>('\r')][static_cast<uint8_t>(CSVState::EMPTY_SPACE)] =
		    CSVState::RECORD_SEPARATOR;
	}
	transition_array[quote][static_cast<uint8_t>(CSVState::EMPTY_SPACE)] = CSVState::QUOTED;
	// Initialize characters we can skip during processing, for Standard and Quoted states
	for (idx_t i = 0; i < StateMachine::NUM_TRANSITIONS; i++) {
		transition_array.skip_standard[i] = true;
		transition_array.skip_quoted[i] = true;
	}
	// For standard states we only care for delimiters \r and \n
	transition_array.skip_standard[delimiter] = false;
	transition_array.skip_standard[static_cast<uint8_t>('\n')] = false;
	transition_array.skip_standard[static_cast<uint8_t>('\r')] = false;

	// For quoted we only care about quote, escape and for delimiters \r and \n
	transition_array.skip_quoted[quote] = false;
	transition_array.skip_quoted[escape] = false;
	transition_array.skip_quoted[static_cast<uint8_t>('\n')] = false;
	transition_array.skip_quoted[static_cast<uint8_t>('\r')] = false;

	transition_array.delimiter = delimiter;
	transition_array.new_line = static_cast<uint8_t>('\n');
	transition_array.carriage_return = static_cast<uint8_t>('\r');
	transition_array.quote = quote;
	transition_array.escape = escape;

	// Shift and OR to replicate across all bytes
	transition_array.delimiter |= transition_array.delimiter << 8;
	transition_array.delimiter |= transition_array.delimiter << 16;
	transition_array.delimiter |= transition_array.delimiter << 32;

	transition_array.new_line |= transition_array.new_line << 8;
	transition_array.new_line |= transition_array.new_line << 16;
	transition_array.new_line |= transition_array.new_line << 32;

	transition_array.carriage_return |= transition_array.carriage_return << 8;
	transition_array.carriage_return |= transition_array.carriage_return << 16;
	transition_array.carriage_return |= transition_array.carriage_return << 32;

	transition_array.quote |= transition_array.quote << 8;
	transition_array.quote |= transition_array.quote << 16;
	transition_array.quote |= transition_array.quote << 32;

	transition_array.escape |= transition_array.escape << 8;
	transition_array.escape |= transition_array.escape << 16;
	transition_array.escape |= transition_array.escape << 32;
}

CSVStateMachineCache::CSVStateMachineCache() {
	for (auto quoterule : default_quote_rule) {
		const auto &quote_candidates = default_quote[static_cast<uint8_t>(quoterule)];
		for (const auto &quote : quote_candidates) {
			for (const auto &delimiter : default_delimiter) {
				const auto &escape_candidates = default_escape[static_cast<uint8_t>(quoterule)];
				for (const auto &escape : escape_candidates) {
					Insert({delimiter, quote, escape, NewLineIdentifier::SINGLE});
					Insert({delimiter, quote, escape, NewLineIdentifier::CARRY_ON});
				}
			}
		}
	}
}

const StateMachine &CSVStateMachineCache::Get(const CSVStateMachineOptions &state_machine_options) {
	// Custom State Machine, we need to create it and cache it first
	lock_guard<mutex> parallel_lock(main_mutex);
	if (state_machine_cache.find(state_machine_options) == state_machine_cache.end()) {
		Insert(state_machine_options);
	}
	const auto &transition_array = state_machine_cache[state_machine_options];
	return transition_array;
}

CSVStateMachineCache &CSVStateMachineCache::Get(ClientContext &context) {

	auto &cache = ObjectCache::GetObjectCache(context);
	return *cache.GetOrCreate<CSVStateMachineCache>(CSVStateMachineCache::ObjectType());
}

} // namespace duckdb
