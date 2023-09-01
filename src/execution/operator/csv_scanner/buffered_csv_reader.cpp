#include "duckdb/execution/operator/scan/csv/buffered_csv_reader.hpp"

#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"
#include "duckdb/common/file_system.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/common/to_string.hpp"
#include "duckdb/common/types/cast_helpers.hpp"
#include "duckdb/common/vector_operations/unary_executor.hpp"
#include "duckdb/common/vector_operations/vector_operations.hpp"
#include "duckdb/execution/operator/scan/csv/csv_sniffer.hpp"
#include "duckdb/execution/operator/scan/csv/csv_state_machine.hpp"
#include "duckdb/function/scalar/strftime_format.hpp"
#include "duckdb/main/client_data.hpp"
#include "duckdb/main/database.hpp"
#include "duckdb/main/error_manager.hpp"
#include "duckdb/parser/column_definition.hpp"
#include "duckdb/parser/keyword_helper.hpp"
#include "duckdb/storage/data_table.hpp"
#include "utf8proc.hpp"
#include "utf8proc_wrapper.hpp"

#include <algorithm>
#include <cctype>
#include <cstring>
#include <fstream>

namespace duckdb {

BufferedCSVReader::BufferedCSVReader(ClientContext &context, CSVReaderOptions options_p,
                                     const vector<LogicalType> &requested_types)
    : BaseCSVReader(context, std::move(options_p), requested_types), buffer_size(0), position(0), start(0) {
	file_handle = OpenCSV(context, options);
	Initialize(requested_types);
}

BufferedCSVReader::BufferedCSVReader(ClientContext &context, string filename, CSVReaderOptions options_p,
                                     const vector<LogicalType> &requested_types)
    : BaseCSVReader(context, std::move(options_p), requested_types), buffer_size(0), position(0), start(0) {
	options.file_path = std::move(filename);
	file_handle = OpenCSV(context, options);
	Initialize(requested_types);
}

void BufferedCSVReader::Initialize(const vector<LogicalType> &requested_types) {
	if (options.auto_detect && options.file_options.union_by_name) {
		// This is required for the sniffer to work on Union By Name
		D_ASSERT(options.file_path == file_handle->GetFilePath());
		auto bm_file_handle = BaseCSVReader::OpenCSV(context, options);
		auto csv_buffer_manager = make_shared<CSVBufferManager>(context, std::move(bm_file_handle), options);
		CSVSniffer sniffer(options, csv_buffer_manager, state_machine_cache);
		auto sniffer_result = sniffer.SniffCSV();
		return_types = sniffer_result.return_types;
		names = sniffer_result.names;
		if (return_types.empty()) {
			throw InvalidInputException("Failed to detect column types from CSV: is the file a valid CSV file?");
		}
	} else {
		return_types = requested_types;
		ResetBuffer();
	}
	SkipRowsAndReadHeader(options.dialect_options.skip_rows, options.dialect_options.header);
	InitParseChunk(return_types.size());
}

void BufferedCSVReader::ResetBuffer() {
	buffer.reset();
	buffer_size = 0;
	position = 0;
	start = 0;
	cached_buffers.clear();
}

void BufferedCSVReader::SkipRowsAndReadHeader(idx_t skip_rows, bool skip_header) {
	for (idx_t i = 0; i < skip_rows; i++) {
		// ignore skip rows
		string read_line = file_handle->ReadLine();
		linenr++;
	}

	if (skip_header) {
		// ignore the first line as a header line
		InitParseChunk(return_types.size());
		ParseCSV(ParserMode::PARSING_HEADER);
	}
}

string BufferedCSVReader::ColumnTypesError(case_insensitive_map_t<idx_t> sql_types_per_column,
                                           const vector<string> &names) {
	for (idx_t i = 0; i < names.size(); i++) {
		auto it = sql_types_per_column.find(names[i]);
		if (it != sql_types_per_column.end()) {
			sql_types_per_column.erase(names[i]);
			continue;
		}
	}
	if (sql_types_per_column.empty()) {
		return string();
	}
	string exception = "COLUMN_TYPES error: Columns with names: ";
	for (auto &col : sql_types_per_column) {
		exception += "\"" + col.first + "\",";
	}
	exception.pop_back();
	exception += " do not exist in the CSV File";
	return exception;
}

void BufferedCSVReader::SkipEmptyLines() {
	if (parse_chunk.data.size() == 1) {
		// Empty lines are null data.
		return;
	}
	for (; position < buffer_size; position++) {
		if (!StringUtil::CharacterIsNewline(buffer[position])) {
			return;
		}
	}
}

void UpdateMaxLineLength(ClientContext &context, idx_t line_length) {
	if (!context.client_data->debug_set_max_line_length) {
		return;
	}
	if (line_length < context.client_data->debug_max_line_length) {
		return;
	}
	context.client_data->debug_max_line_length = line_length;
}

bool BufferedCSVReader::ReadBuffer(idx_t &start, idx_t &line_start) {
	if (start > buffer_size) {
		return false;
	}
	auto old_buffer = std::move(buffer);

	// the remaining part of the last buffer
	idx_t remaining = buffer_size - start;

	idx_t buffer_read_size = INITIAL_BUFFER_SIZE_LARGE;

	while (remaining > buffer_read_size) {
		buffer_read_size *= 2;
	}

	// Check line length
	if (remaining > options.maximum_line_size) {
		throw InvalidInputException("Maximum line size of %llu bytes exceeded on line %s!", options.maximum_line_size,
		                            GetLineNumberStr(linenr, linenr_estimated));
	}

	buffer = make_unsafe_uniq_array<char>(buffer_read_size + remaining + 1);
	buffer_size = remaining + buffer_read_size;
	if (remaining > 0) {
		// remaining from last buffer: copy it here
		memcpy(buffer.get(), old_buffer.get() + start, remaining);
	}
	idx_t read_count = file_handle->Read(buffer.get() + remaining, buffer_read_size);

	bytes_in_chunk += read_count;
	buffer_size = remaining + read_count;
	buffer[buffer_size] = '\0';
	if (old_buffer) {
		cached_buffers.push_back(std::move(old_buffer));
	}
	start = 0;
	position = remaining;
	if (!bom_checked) {
		bom_checked = true;
		if (read_count >= 3 && buffer[0] == '\xEF' && buffer[1] == '\xBB' && buffer[2] == '\xBF') {
			start += 3;
			position += 3;
		}
	}
	line_start = start;

	return read_count > 0;
}

void BufferedCSVReader::ParseCSV(DataChunk &insert_chunk) {
	string error_message;
	if (!TryParseCSV(ParserMode::PARSING, insert_chunk, error_message)) {
		throw InvalidInputException(error_message);
	}
}

void BufferedCSVReader::ParseCSV(ParserMode mode) {
	DataChunk dummy_chunk;
	string error_message;
	if (!TryParseCSV(mode, dummy_chunk, error_message)) {
		throw InvalidInputException(error_message);
	}
}

bool BufferedCSVReader::TryParseCSV(ParserMode parser_mode, DataChunk &insert_chunk, string &error_message) {
	mode = parser_mode;
	// used for parsing algorithm
	bool finished_chunk = false;
	idx_t column = 0;
	idx_t offset = 0;
	bool has_quotes = false;
	vector<idx_t> escape_positions;

	idx_t line_start = position;
	idx_t line_size = 0;
	// read values into the buffer (if any)
	if (position >= buffer_size) {
		if (!ReadBuffer(start, line_start)) {
			return true;
		}
	}

	// start parsing the first value
	goto value_start;
value_start:
	offset = 0;
	/* state: value_start */
	// this state parses the first character of a value
	if (buffer[position] == options.dialect_options.state_machine_options.quote) {
		// quote: actual value starts in the next position
		// move to in_quotes state
		start = position + 1;
		line_size++;
		goto in_quotes;
	} else {
		// no quote, move to normal parsing state
		start = position;
		goto normal;
	}
normal:
	/* state: normal parsing state */
	// this state parses the remainder of a non-quoted value until we reach a delimiter or newline
	do {
		for (; position < buffer_size; position++) {
			line_size++;
			if (buffer[position] == options.dialect_options.state_machine_options.delimiter) {
				// delimiter: end the value and add it to the chunk
				goto add_value;
			} else if (StringUtil::CharacterIsNewline(buffer[position])) {
				// newline: add row
				goto add_row;
			}
		}
	} while (ReadBuffer(start, line_start));
	// file ends during normal scan: go to end state
	goto final_state;
add_value:
	AddValue(string_t(buffer.get() + start, position - start - offset), column, escape_positions, has_quotes);
	// increase position by 1 and move start to the new position
	offset = 0;
	has_quotes = false;
	start = ++position;
	line_size++;
	if (position >= buffer_size && !ReadBuffer(start, line_start)) {
		// file ends right after delimiter, go to final state
		goto final_state;
	}
	goto value_start;
add_row : {
	// check type of newline (\r or \n)
	bool carriage_return = buffer[position] == '\r';
	AddValue(string_t(buffer.get() + start, position - start - offset), column, escape_positions, has_quotes);
	if (!error_message.empty()) {
		return false;
	}
	VerifyLineLength(position - line_start);

	finished_chunk = AddRow(insert_chunk, column, error_message);
	UpdateMaxLineLength(context, position - line_start);
	if (!error_message.empty()) {
		return false;
	}
	// increase position by 1 and move start to the new position
	offset = 0;
	has_quotes = false;
	position++;
	line_size = 0;
	start = position;
	line_start = position;
	if (position >= buffer_size && !ReadBuffer(start, line_start)) {
		// file ends right after delimiter, go to final state
		goto final_state;
	}
	if (carriage_return) {
		// \r newline, go to special state that parses an optional \n afterwards
		goto carriage_return;
	} else {
		SetNewLineDelimiter();
		SkipEmptyLines();

		start = position;
		line_start = position;
		if (position >= buffer_size && !ReadBuffer(start, line_start)) {
			// file ends right after delimiter, go to final state
			goto final_state;
		}
		// \n newline, move to value start
		if (finished_chunk) {
			return true;
		}
		goto value_start;
	}
}
in_quotes:
	/* state: in_quotes */
	// this state parses the remainder of a quoted value
	has_quotes = true;
	position++;
	line_size++;
	do {
		for (; position < buffer_size; position++) {
			line_size++;
			if (buffer[position] == options.dialect_options.state_machine_options.quote) {
				// quote: move to unquoted state
				goto unquote;
			} else if (buffer[position] == options.dialect_options.state_machine_options.escape) {
				// escape: store the escaped position and move to handle_escape state
				escape_positions.push_back(position - start);
				goto handle_escape;
			}
		}
	} while (ReadBuffer(start, line_start));
	// still in quoted state at the end of the file, error:
	throw InvalidInputException("Error in file \"%s\" on line %s: unterminated quotes. (%s)", options.file_path,
	                            GetLineNumberStr(linenr, linenr_estimated).c_str(), options.ToString());
unquote:
	/* state: unquote */
	// this state handles the state directly after we unquote
	// in this state we expect either another quote (entering the quoted state again, and escaping the quote)
	// or a delimiter/newline, ending the current value and moving on to the next value
	position++;
	line_size++;
	if (position >= buffer_size && !ReadBuffer(start, line_start)) {
		// file ends right after unquote, go to final state
		offset = 1;
		goto final_state;
	}
	if (buffer[position] == options.dialect_options.state_machine_options.quote &&
	    (options.dialect_options.state_machine_options.escape == '\0' ||
	     options.dialect_options.state_machine_options.escape == options.dialect_options.state_machine_options.quote)) {
		// escaped quote, return to quoted state and store escape position
		escape_positions.push_back(position - start);
		goto in_quotes;
	} else if (buffer[position] == options.dialect_options.state_machine_options.delimiter) {
		// delimiter, add value
		offset = 1;
		goto add_value;
	} else if (StringUtil::CharacterIsNewline(buffer[position])) {
		offset = 1;
		goto add_row;
	} else {
		error_message = StringUtil::Format(
		    "Error in file \"%s\" on line %s: quote should be followed by end of value, end of "
		    "row or another quote. (%s)",
		    options.file_path, GetLineNumberStr(linenr, linenr_estimated).c_str(), options.ToString());
		return false;
	}
handle_escape:
	/* state: handle_escape */
	// escape should be followed by a quote or another escape character
	position++;
	line_size++;
	if (position >= buffer_size && !ReadBuffer(start, line_start)) {
		error_message = StringUtil::Format(
		    "Error in file \"%s\" on line %s: neither QUOTE nor ESCAPE is proceeded by ESCAPE. (%s)", options.file_path,
		    GetLineNumberStr(linenr, linenr_estimated).c_str(), options.ToString());
		return false;
	}
	if (buffer[position] != options.dialect_options.state_machine_options.quote &&
	    buffer[position] != options.dialect_options.state_machine_options.escape) {
		error_message = StringUtil::Format(
		    "Error in file \"%s\" on line %s: neither QUOTE nor ESCAPE is proceeded by ESCAPE. (%s)", options.file_path,
		    GetLineNumberStr(linenr, linenr_estimated).c_str(), options.ToString());
		return false;
	}
	// escape was followed by quote or escape, go back to quoted state
	goto in_quotes;
carriage_return:
	/* state: carriage_return */
	// this stage optionally skips a newline (\n) character, which allows \r\n to be interpreted as a single line
	if (buffer[position] == '\n') {
		SetNewLineDelimiter(true, true);
		// newline after carriage return: skip
		// increase position by 1 and move start to the new position
		start = ++position;
		line_size++;

		if (position >= buffer_size && !ReadBuffer(start, line_start)) {
			// file ends right after delimiter, go to final state
			goto final_state;
		}
	} else {
		SetNewLineDelimiter(true, false);
	}
	if (finished_chunk) {
		return true;
	}
	SkipEmptyLines();
	start = position;
	line_start = position;
	if (position >= buffer_size && !ReadBuffer(start, line_start)) {
		// file ends right after delimiter, go to final state
		goto final_state;
	}

	goto value_start;
final_state:
	if (finished_chunk) {
		return true;
	}

	if (column > 0 || position > start) {
		// remaining values to be added to the chunk
		AddValue(string_t(buffer.get() + start, position - start - offset), column, escape_positions, has_quotes);
		VerifyLineLength(position - line_start);

		finished_chunk = AddRow(insert_chunk, column, error_message);
		SkipEmptyLines();
		UpdateMaxLineLength(context, line_size);
		if (!error_message.empty()) {
			return false;
		}
	}

	// final stage, only reached after parsing the file is finished
	// flush the parsed chunk and finalize parsing
	if (mode == ParserMode::PARSING) {
		Flush(insert_chunk);
	}

	end_of_file_reached = true;
	return true;
}

} // namespace duckdb
