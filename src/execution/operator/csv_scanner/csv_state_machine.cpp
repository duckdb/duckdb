#include "duckdb/execution/operator/persistent/csv_scanner/csv_state_machine.hpp"
#include "utf8proc_wrapper.hpp"
#include "duckdb/main/error_manager.hpp"

namespace duckdb {
CSVStateMachine::CSVStateMachine(CSVReaderOptions options_p, shared_ptr<CSVBufferManager> buffer_manager_p)
    : options(std::move(options_p)), csv_buffer_iterator(std::move(buffer_manager_p)) {
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
	transition_array[standard_state][static_cast<uint8_t>(options.delimiter)] = field_separator_state;
	transition_array[standard_state][static_cast<uint8_t>('\n')] = record_separator_state;
	transition_array[standard_state][static_cast<uint8_t>('\r')] = carriage_return_state;
	transition_array[standard_state][static_cast<uint8_t>(options.quote)] = quoted_state;
	// 2) Field Separator State
	transition_array[field_separator_state][static_cast<uint8_t>(options.delimiter)] = field_separator_state;
	transition_array[field_separator_state][static_cast<uint8_t>('\n')] = record_separator_state;
	transition_array[field_separator_state][static_cast<uint8_t>('\r')] = carriage_return_state;
	transition_array[field_separator_state][static_cast<uint8_t>(options.quote)] = quoted_state;
	// 3) Record Separator State
	transition_array[record_separator_state][static_cast<uint8_t>(options.delimiter)] = field_separator_state;
	transition_array[record_separator_state][static_cast<uint8_t>('\n')] = record_separator_state;
	transition_array[record_separator_state][static_cast<uint8_t>('\r')] = carriage_return_state;
	transition_array[record_separator_state][static_cast<uint8_t>(options.quote)] = quoted_state;
	// 4) Carriage Return State
	transition_array[carriage_return_state][static_cast<uint8_t>('\n')] = record_separator_state;
	transition_array[carriage_return_state][static_cast<uint8_t>('\r')] = carriage_return_state;
	transition_array[carriage_return_state][static_cast<uint8_t>(options.escape)] = escape_state;
	// 5) Quoted State
	for (int j = 0; j < 256; j++) {
		transition_array[quoted_state][j] = quoted_state;
	}
	transition_array[quoted_state][static_cast<uint8_t>(options.quote)] = unquoted_state;

	if (options.quote != options.escape) {
		transition_array[quoted_state][static_cast<uint8_t>(options.escape)] = escape_state;
	}
	// 6) Unquoted State
	for (int j = 0; j < 256; j++) {
		transition_array[unquoted_state][j] = invalid_state;
	}
	transition_array[unquoted_state][static_cast<uint8_t>('\n')] = record_separator_state;
	transition_array[unquoted_state][static_cast<uint8_t>('\r')] = carriage_return_state;
	transition_array[unquoted_state][static_cast<uint8_t>(options.delimiter)] = field_separator_state;
	if (options.quote == options.escape) {
		transition_array[unquoted_state][static_cast<uint8_t>(options.escape)] = quoted_state;
	}

	// 7) Escaped State
	for (int j = 0; j < 256; j++) {
		// Escape is always invalid if not proceeded by another escape or quoted char
		transition_array[escape_state][j] = invalid_state;
	}
	transition_array[escape_state][static_cast<uint8_t>(options.quote)] = quoted_state;
	transition_array[escape_state][static_cast<uint8_t>(options.escape)] = quoted_state;
}

void CSVStateMachine::Reset() {
	csv_buffer_iterator.Reset();
}

void CSVStateMachine::VerifyUTF8() {
	auto utf_type = Utf8Proc::Analyze(value.c_str(), value.size());
	if (utf_type == UnicodeType::INVALID) {
		int64_t error_line = cur_rows;
		throw InvalidInputException("Error in file \"%s\" at line %llu: "
		                            "%s. Parser options:\n%s",
		                            options.file_path, error_line, ErrorManager::InvalidUnicodeError(value, "CSV file"),
		                            options.ToString());
	}
}
} // namespace duckdb
