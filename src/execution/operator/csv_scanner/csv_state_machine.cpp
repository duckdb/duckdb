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

void CSVStateMachine::Reset(bool get_next_char) {
	csv_buffer_iterator.Reset();
	if (get_next_char) {
		current_char = csv_buffer_iterator.GetNextChar();
	}
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

void CSVStateMachine::Parse(DataChunk &parse_chunk) {
	CSVState state {CSVState::STANDARD};
	CSVState previous_state {CSVState::STANDARD};
	CSVState pre_previous_state {CSVState::STANDARD};

	idx_t cur_row = 0;
	idx_t cur_col = 0;
	string value;
	while (!csv_buffer_iterator.Finished()) {
		pre_previous_state = previous_state;
		previous_state = state;
		state =
		    static_cast<CSVState>(transition_array[static_cast<uint8_t>(state)][static_cast<uint8_t>(current_char)]);
		bool empty_line =
		    (state == CSVState::CARRIAGE_RETURN && previous_state == CSVState::CARRIAGE_RETURN) ||
		    (state == CSVState::RECORD_SEPARATOR && previous_state == CSVState::RECORD_SEPARATOR) ||
		    (state == CSVState::CARRIAGE_RETURN && previous_state == CSVState::RECORD_SEPARATOR) ||
		    (pre_previous_state == CSVState::RECORD_SEPARATOR && previous_state == CSVState::CARRIAGE_RETURN);

		bool carriage_return = previous_state == CSVState::CARRIAGE_RETURN;
		if (previous_state == CSVState::FIELD_SEPARATOR ||
		    (previous_state == CSVState::RECORD_SEPARATOR && !empty_line) ||
		    (state != CSVState::RECORD_SEPARATOR && carriage_return)) {
			// Started a new value
			// Check if it's UTF-8 (Or not?)
			VerifyUTF8();
			auto &v = parse_chunk.data[cur_col++];
			auto parse_data = FlatVector::GetData<string_t>(v);
			auto &validity_mask = FlatVector::Validity(v);
			if (value.empty()) {
				validity_mask.SetInvalid(cur_row);
			} else {
				parse_data[cur_row] = StringVector::AddStringOrBlob(v, string_t(value));
			}
			value = "";
		}
		if (((previous_state == CSVState::RECORD_SEPARATOR && !empty_line) ||
		     (state != CSVState::RECORD_SEPARATOR && carriage_return)) &&
		    options.null_padding && cur_col < parse_chunk.ColumnCount()) {
			// It's a new row, check if we need to pad stuff
			while (cur_col < parse_chunk.ColumnCount()) {
				auto &v = parse_chunk.data[cur_col++];
				auto &validity_mask = FlatVector::Validity(v);
				validity_mask.SetInvalid(cur_row);
			}
		}
		if (state == CSVState::STANDARD) {
			value += current_char;
		}
		cur_row += previous_state == CSVState::RECORD_SEPARATOR && !empty_line;
		cur_col -= cur_col * (previous_state == CSVState::RECORD_SEPARATOR);

		// It means our carriage return is actually a record separator
		cur_row += state != CSVState::RECORD_SEPARATOR && carriage_return;
		cur_col -= cur_col * (state != CSVState::RECORD_SEPARATOR && carriage_return);

		if (cur_row >= options.sample_chunk_size) {
			// We sniffed enough rows
			break;
		}
		current_char = csv_buffer_iterator.GetNextChar();
	}
	bool empty_line = (state == CSVState::CARRIAGE_RETURN && previous_state == CSVState::CARRIAGE_RETURN) ||
	                  (state == CSVState::RECORD_SEPARATOR && previous_state == CSVState::RECORD_SEPARATOR) ||
	                  (state == CSVState::CARRIAGE_RETURN && previous_state == CSVState::RECORD_SEPARATOR) ||
	                  (pre_previous_state == CSVState::RECORD_SEPARATOR && previous_state == CSVState::CARRIAGE_RETURN);
	if (cur_row < options.sample_chunk_size && !empty_line) {
		VerifyUTF8();
		auto &v = parse_chunk.data[cur_col++];
		auto parse_data = FlatVector::GetData<string_t>(v);
		parse_data[cur_row] = StringVector::AddStringOrBlob(v, string_t(value));
	}
	parse_chunk.SetCardinality(cur_row);
}

} // namespace duckdb
