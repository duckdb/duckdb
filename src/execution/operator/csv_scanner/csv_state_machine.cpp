#include <utility>

#include "duckdb/execution/operator/persistent/csv_scanner/csv_state_machine.hpp"

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
	current_char = csv_buffer_iterator.GetNextChar();
}

void CSVStateMachine::Reset() {
	csv_buffer_iterator.Reset();
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
	if (options.new_line == NewLineIdentifier::NOT_SET) {
		options.new_line = suggested_newline;
	} else {
		if (options.new_line != suggested_newline) {
			// Invalidate this whole detection
			cur_rows = 0;
		}
	}
	sniffed_column_counts.erase(sniffed_column_counts.end() - (STANDARD_VECTOR_SIZE - cur_rows),
	                            sniffed_column_counts.end());
}
void VerifyUTF8(CSVReaderOptions &options, idx_t linenr, string &value) {
	auto utf_type = Utf8Proc::Analyze(value.c_str(), value.size());
	if (utf_type == UnicodeType::INVALID) {
		int64_t error_line = linenr;
		throw InvalidInputException("Error in file \"%s\" at line %llu: "
		                            "%s. Parser options:\n%s",
		                            options.file_path, error_line, ErrorManager::InvalidUnicodeError(value, "CSV file"),
		                            options.ToString());
	}
}

void CSVStateMachine::SniffValue(vector<vector<Value>> &sniffed_value) {
	CSVState state {CSVState::STANDARD};
	CSVState previous_state;
	idx_t cur_row = 0;
	D_ASSERT(sniffed_value.size() == STANDARD_VECTOR_SIZE);
	string value;
	while (!csv_buffer_iterator.Finished()) {
		previous_state = state;
		state =
		    static_cast<CSVState>(transition_array[static_cast<uint8_t>(state)][static_cast<uint8_t>(current_char)]);
		bool empty_line = (state == CSVState::CARRIAGE_RETURN && previous_state == CSVState::CARRIAGE_RETURN) ||
		                  (state == CSVState::RECORD_SEPARATOR && previous_state == CSVState::RECORD_SEPARATOR);

		bool carriage_return = previous_state == CSVState::CARRIAGE_RETURN;
		if (previous_state == CSVState::FIELD_SEPARATOR ||
		    (previous_state == CSVState::RECORD_SEPARATOR && !empty_line) ||
		    (state != CSVState::RECORD_SEPARATOR && carriage_return)) {
			// Started a new value
			// Check if it's UTF-8
			VerifyUTF8(options, cur_row, value);
			sniffed_value[cur_row].push_back(Value(value));
			value = current_char;
		}
		if (state == CSVState::STANDARD) {
			value += current_char;
		}
		cur_row += previous_state == CSVState::RECORD_SEPARATOR && !empty_line;
		// It means our carriage return is actually a record separator
		cur_row += state != CSVState::RECORD_SEPARATOR && carriage_return;
		if (cur_row >= STANDARD_VECTOR_SIZE) {
			// We sniffed enough rows
			break;
		}
		current_char = csv_buffer_iterator.GetNextChar();
	}
	bool empty_line = (state == CSVState::CARRIAGE_RETURN && previous_state == CSVState::CARRIAGE_RETURN) ||
	                  (state == CSVState::RECORD_SEPARATOR && previous_state == CSVState::RECORD_SEPARATOR);
	if (cur_row < STANDARD_VECTOR_SIZE && !empty_line) {
		VerifyUTF8(options, cur_row, value);

		sniffed_value[cur_row++].push_back(Value(value));
	}
	sniffed_value.erase(sniffed_value.end() - (STANDARD_VECTOR_SIZE - cur_row), sniffed_value.end());
}

void CSVStateMachine::Parse(DataChunk &parse_chunk) {
	CSVState state {CSVState::STANDARD};
	CSVState previous_state;
	idx_t cur_row = 0;
	idx_t cur_col = 0;
	string value;
	while (!csv_buffer_iterator.Finished()) {
		previous_state = state;
		state =
		    static_cast<CSVState>(transition_array[static_cast<uint8_t>(state)][static_cast<uint8_t>(current_char)]);
		bool empty_line = (state == CSVState::CARRIAGE_RETURN && previous_state == CSVState::CARRIAGE_RETURN) ||
		                  (state == CSVState::RECORD_SEPARATOR && previous_state == CSVState::RECORD_SEPARATOR);

		bool carriage_return = previous_state == CSVState::CARRIAGE_RETURN;
		if (previous_state == CSVState::FIELD_SEPARATOR ||
		    (previous_state == CSVState::RECORD_SEPARATOR && !empty_line) ||
		    (state != CSVState::RECORD_SEPARATOR && carriage_return)) {
			// Started a new value
			// Check if it's UTF-8
			VerifyUTF8(options, cur_row, value);
			auto &v = parse_chunk.data[cur_col++];
			auto parse_data = FlatVector::GetData<string_t>(v);
			parse_data[cur_row] = StringVector::AddStringOrBlob(v, string_t(value));
			value = current_char;
		}
		if (state == CSVState::STANDARD) {
			value += current_char;
		}
		cur_row += previous_state == CSVState::RECORD_SEPARATOR && !empty_line;
		cur_col -= cur_col * (previous_state == CSVState::RECORD_SEPARATOR);

		// It means our carriage return is actually a record separator
		cur_row += state != CSVState::RECORD_SEPARATOR && carriage_return;
		cur_col -= cur_col * (state != CSVState::RECORD_SEPARATOR && carriage_return);

		if (cur_row >= STANDARD_VECTOR_SIZE) {
			// We sniffed enough rows
			break;
		}
		current_char = csv_buffer_iterator.GetNextChar();
	}
	bool empty_line = (state == CSVState::CARRIAGE_RETURN && previous_state == CSVState::CARRIAGE_RETURN) ||
	                  (state == CSVState::RECORD_SEPARATOR && previous_state == CSVState::RECORD_SEPARATOR);
	if (cur_row < STANDARD_VECTOR_SIZE && !empty_line) {
		VerifyUTF8(options, cur_row, value);
		auto &v = parse_chunk.data[cur_col++];
		auto parse_data = FlatVector::GetData<string_t>(v);
		parse_data[cur_row] = StringVector::AddStringOrBlob(v, string_t(value));
	}
}

} // namespace duckdb
