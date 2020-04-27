#include "duckdb/execution/operator/persistent/buffered_csv_reader.hpp"
#include "duckdb/execution/operator/persistent/physical_copy_from_file.hpp"

#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"
#include "duckdb/common/vector_operations/vector_operations.hpp"
#include "duckdb/main/database.hpp"
#include "duckdb/storage/data_table.hpp"
#include "duckdb/parser/column_definition.hpp"

#include "duckdb/common/file_system.hpp"
#include "duckdb/common/gzip_stream.hpp"
#include "duckdb/common/string_util.hpp"

#include "utf8proc_wrapper.hpp"

#include <algorithm>
#include <fstream>
#include <queue>
#include <cstring>

using namespace duckdb;
using namespace std;

static char is_newline(char c) {
	return c == '\n' || c == '\r';
}

TextSearchShiftArray::TextSearchShiftArray(string search_term) : length(search_term.size()) {
	if (length > 255) {
		throw Exception("Size of delimiter/quote/escape in CSV reader is limited to 255 bytes");
	}
	// initialize the shifts array
	shifts = unique_ptr<uint8_t[]>(new uint8_t[length * 255]);
	memset(shifts.get(), 0, length * 255 * sizeof(uint8_t));
	// iterate over each of the characters in the array
	for (idx_t main_idx = 0; main_idx < length; main_idx++) {
		uint8_t current_char = (uint8_t)search_term[main_idx];
		// now move over all the remaining positions
		for (idx_t i = main_idx; i < length; i++) {
			bool is_match = true;
			// check if the prefix matches at this position
			// if it does, we move to this position after encountering the current character
			for (idx_t j = 0; j < main_idx; j++) {
				if (search_term[i - main_idx + j] != search_term[j]) {
					is_match = false;
				}
			}
			if (!is_match) {
				continue;
			}
			shifts[i * 255 + current_char] = main_idx + 1;
		}
	}
}

BufferedCSVReader::BufferedCSVReader(ClientContext &context, CopyInfo &info, vector<SQLType> sql_types)
    : BufferedCSVReader(info, sql_types, OpenCSV(context, info)) {
}

BufferedCSVReader::BufferedCSVReader(CopyInfo &info, vector<SQLType> sql_types, unique_ptr<istream> ssource)
    : info(info), sql_types(sql_types), source(move(ssource)), buffer_size(0), position(0), start(0),
      delimiter_search(info.delimiter), escape_search(info.escape), quote_search(info.quote) {
	if (info.force_not_null.size() == 0) {
		info.force_not_null.resize(sql_types.size(), false);
	}

	assert(info.force_not_null.size() == sql_types.size());
	// initialize the parse_chunk with a set of VARCHAR types
	vector<TypeId> varchar_types;
	for (idx_t i = 0; i < sql_types.size(); i++) {
		varchar_types.push_back(TypeId::VARCHAR);
	}
	parse_chunk.Initialize(varchar_types);

	if (info.header) {
		// ignore the first line as a header line
		string read_line;
		getline(*source, read_line);
		linenr++;
	}
}

unique_ptr<istream> BufferedCSVReader::OpenCSV(ClientContext &context, CopyInfo &info) {
	if (!FileSystem::GetFileSystem(context).FileExists(info.file_path)) {
		throw IOException("File \"%s\" not found", info.file_path.c_str());
	}
	unique_ptr<istream> result;
	// decide based on the extension which stream to use
	if (StringUtil::EndsWith(StringUtil::Lower(info.file_path), ".gz")) {
		result = make_unique<GzipStream>(info.file_path);
	} else {
		auto csv_local = make_unique<ifstream>();
		csv_local->open(info.file_path);
		result = move(csv_local);
	}
	return result;
}

void BufferedCSVReader::ParseComplexCSV(DataChunk &insert_chunk) {
	// used for parsing algorithm
	bool finished_chunk = false;
	idx_t column = 0;
	vector<idx_t> escape_positions;
	uint8_t delimiter_pos = 0, escape_pos = 0, quote_pos = 0;
	idx_t offset = 0;

	// read values into the buffer (if any)
	if (position >= buffer_size) {
		if (!ReadBuffer(start)) {
			return;
		}
	}
	// start parsing the first value
	start = position;
	goto value_start;
value_start:
	/* state: value_start */
	// this state parses the first characters of a value
	offset = 0;
	delimiter_pos = 0;
	quote_pos = 0;
	do {
		idx_t count = 0;
		for (; position < buffer_size; position++) {
			quote_search.Match(quote_pos, buffer[position]);
			delimiter_search.Match(delimiter_pos, buffer[position]);
			count++;
			if (delimiter_pos == info.delimiter.size()) {
				// found a delimiter, add the value
				offset = info.delimiter.size() - 1;
				goto add_value;
			} else if (is_newline(buffer[position])) {
				// found a newline, add the row
				goto add_row;
			}
			if (count > quote_pos) {
				// did not find a quote directly at the start of the value, stop looking for the quote now
				goto normal;
			}
			if (quote_pos == info.quote.size()) {
				// found a quote, go to quoted loop and skip the initial quote
				start += info.quote.size();
				goto in_quotes;
			}
		}
	} while (ReadBuffer(start));
	// file ends while scanning for quote/delimiter, go to final state
	goto final_state;
normal:
	/* state: normal parsing state */
	// this state parses the remainder of a non-quoted value until we reach a delimiter or newline
	position++;
	do {
		for (; position < buffer_size; position++) {
			delimiter_search.Match(delimiter_pos, buffer[position]);
			if (delimiter_pos == info.delimiter.size()) {
				offset = info.delimiter.size() - 1;
				goto add_value;
			} else if (is_newline(buffer[position])) {
				goto add_row;
			}
		}
	} while (ReadBuffer(start));
	goto final_state;
add_value:
	AddValue(buffer.get() + start, position - start - offset, column, escape_positions);
	// increase position by 1 and move start to the new position
	offset = 0;
	start = ++position;
	if (position >= buffer_size && !ReadBuffer(start)) {
		// file ends right after delimiter, go to final state
		goto final_state;
	}
	goto value_start;
add_row : {
	// check type of newline (\r or \n)
	bool carriage_return = buffer[position] == '\r';
	AddValue(buffer.get() + start, position - start - offset, column, escape_positions);
	finished_chunk = AddRow(insert_chunk, column);
	// increase position by 1 and move start to the new position
	offset = 0;
	start = ++position;
	if (position >= buffer_size && !ReadBuffer(start)) {
		// file ends right after newline, go to final state
		goto final_state;
	}
	if (carriage_return) {
		// \r newline, go to special state that parses an optional \n afterwards
		goto carriage_return;
	} else {
		// \n newline, move to value start
		if (finished_chunk) {
			return;
		}
		goto value_start;
	}
}
in_quotes:
	/* state: in_quotes */
	// this state parses the remainder of a quoted value
	quote_pos = 0;
	escape_pos = 0;
	position++;
	do {
		for (; position < buffer_size; position++) {
			quote_search.Match(quote_pos, buffer[position]);
			escape_search.Match(escape_pos, buffer[position]);
			if (quote_pos == info.quote.size()) {
				goto unquote;
			} else if (escape_pos == info.escape.size()) {
				escape_positions.push_back(position - start - (info.escape.size() - 1));
				goto handle_escape;
			}
		}
	} while (ReadBuffer(start));
	// still in quoted state at the end of the file, error:
	throw ParserException("Error on line %lld: unterminated quotes", linenr);
unquote:
	/* state: unquote */
	// this state handles the state directly after we unquote
	// in this state we expect either another quote (entering the quoted state again, and escaping the quote)
	// or a delimiter/newline, ending the current value and moving on to the next value
	delimiter_pos = 0;
	quote_pos = 0;
	position++;
	if (position >= buffer_size && !ReadBuffer(start)) {
		// file ends right after unquote, go to final state
		offset = info.quote.size();
		goto final_state;
	}
	if (is_newline(buffer[position])) {
		// quote followed by newline, add row
		offset = info.quote.size();
		goto add_row;
	}
	do {
		idx_t count = 0;
		for (; position < buffer_size; position++) {
			quote_search.Match(quote_pos, buffer[position]);
			delimiter_search.Match(delimiter_pos, buffer[position]);
			count++;
			if (count > delimiter_pos && count > quote_pos) {
				throw ParserException(
				    "Error on line %lld: quote should be followed by end of value, end of row or another quote",
				    linenr);
			}
			if (delimiter_pos == info.delimiter.size()) {
				// quote followed by delimiter, add value
				offset = info.quote.size() + info.delimiter.size() - 1;
				goto add_value;
			} else if (quote_pos == info.quote.size()) {
				// quote followed by quote, go back to quoted state and add to escape
				escape_positions.push_back(position - start - (info.quote.size() - 1));
				goto in_quotes;
			}
		}
	} while (ReadBuffer(start));
	throw ParserException("Error on line %lld: quote should be followed by end of value, end of row or another quote",
	                      linenr);
handle_escape:
	escape_pos = 0;
	quote_pos = 0;
	position++;
	do {
		idx_t count = 0;
		for (; position < buffer_size; position++) {
			quote_search.Match(quote_pos, buffer[position]);
			escape_search.Match(escape_pos, buffer[position]);
			count++;
			if (count > escape_pos && count > quote_pos) {
				throw ParserException("Error on line %lld: neither QUOTE nor ESCAPE is proceeded by ESCAPE", linenr);
			}
			if (quote_pos == info.quote.size() || escape_pos == info.escape.size()) {
				// found quote or escape: move back to quoted state
				goto in_quotes;
			}
		}
	} while (ReadBuffer(start));
	throw ParserException("Error on line %lld: neither QUOTE nor ESCAPE is proceeded by ESCAPE", linenr);
carriage_return:
	/* state: carriage_return */
	// this stage optionally skips a newline (\n) character, which allows \r\n to be interpreted as a single line
	if (buffer[position] == '\n') {
		// newline after carriage return: skip
		start = ++position;
		if (position >= buffer_size && !ReadBuffer(start)) {
			// file ends right after newline, go to final state
			goto final_state;
		}
	}
	if (finished_chunk) {
		return;
	}
	goto value_start;
final_state:
	if (finished_chunk) {
		return;
	}
	if (column > 0 || position > start) {
		// remaining values to be added to the chunk
		AddValue(buffer.get() + start, position - start - offset, column, escape_positions);
		finished_chunk = AddRow(insert_chunk, column);
	}
	// final stage, only reached after parsing the file is finished
	// flush the parsed chunk and finalize parsing
	Flush(insert_chunk);
}

void BufferedCSVReader::ParseSimpleCSV(DataChunk &insert_chunk) {
	// used for parsing algorithm
	bool finished_chunk = false;
	idx_t column = 0;
	idx_t offset = 0;
	vector<idx_t> escape_positions;

	// read values into the buffer (if any)
	if (position >= buffer_size) {
		if (!ReadBuffer(start)) {
			return;
		}
	}
	// start parsing the first value
	goto value_start;
value_start:
	offset = 0;
	/* state: value_start */
	// this state parses the first character of a value
	if (buffer[position] == info.quote[0]) {
		// quote: actual value starts in the next position
		// move to in_quotes state
		start = position + 1;
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
			if (buffer[position] == info.delimiter[0]) {
				// delimiter: end the value and add it to the chunk
				goto add_value;
			} else if (is_newline(buffer[position])) {
				// newline: add row
				goto add_row;
			}
		}
	} while (ReadBuffer(start));
	// file ends during normal scan: go to end state
	goto final_state;
add_value:
	AddValue(buffer.get() + start, position - start - offset, column, escape_positions);
	// increase position by 1 and move start to the new position
	offset = 0;
	start = ++position;
	if (position >= buffer_size && !ReadBuffer(start)) {
		// file ends right after delimiter, go to final state
		goto final_state;
	}
	goto value_start;
add_row : {
	// check type of newline (\r or \n)
	bool carriage_return = buffer[position] == '\r';
	AddValue(buffer.get() + start, position - start - offset, column, escape_positions);
	finished_chunk = AddRow(insert_chunk, column);
	// increase position by 1 and move start to the new position
	offset = 0;
	start = ++position;
	if (position >= buffer_size && !ReadBuffer(start)) {
		// file ends right after delimiter, go to final state
		goto final_state;
	}
	if (carriage_return) {
		// \r newline, go to special state that parses an optional \n afterwards
		goto carriage_return;
	} else {
		// \n newline, move to value start
		if (finished_chunk) {
			return;
		}
		goto value_start;
	}
}
in_quotes:
	/* state: in_quotes */
	// this state parses the remainder of a quoted value
	position++;
	do {
		for (; position < buffer_size; position++) {
			if (buffer[position] == info.quote[0]) {
				// quote: move to unquoted state
				goto unquote;
			} else if (buffer[position] == info.escape[0]) {
				// escape: store the escaped position and move to handle_escape state
				escape_positions.push_back(position - start);
				goto handle_escape;
			}
		}
	} while (ReadBuffer(start));
	// still in quoted state at the end of the file, error:
	throw ParserException("Error on line %lld: unterminated quotes", linenr);
unquote:
	/* state: unquote */
	// this state handles the state directly after we unquote
	// in this state we expect either another quote (entering the quoted state again, and escaping the quote)
	// or a delimiter/newline, ending the current value and moving on to the next value
	position++;
	if (position >= buffer_size && !ReadBuffer(start)) {
		// file ends right after unquote, go to final state
		offset = 1;
		goto final_state;
	}
	if (buffer[position] == info.quote[0]) {
		// escaped quote, return to quoted state and store escape position
		escape_positions.push_back(position - start);
		goto in_quotes;
	} else if (buffer[position] == info.delimiter[0]) {
		// delimiter, add value
		offset = 1;
		goto add_value;
	} else if (is_newline(buffer[position])) {
		offset = 1;
		goto add_row;
	} else {
		throw ParserException(
		    "Error on line %lld: quote should be followed by end of value, end of row or another quote", linenr);
	}
handle_escape:
	/* state: handle_escape */
	// escape should be followed by a quote or another escape character
	position++;
	if (position >= buffer_size && !ReadBuffer(start)) {
		throw ParserException("Error on line %lld: neither QUOTE nor ESCAPE is proceeded by ESCAPE", linenr);
	}
	if (buffer[position] != info.quote[0] && buffer[position] != info.escape[0]) {
		throw ParserException("Error on line %lld: neither QUOTE nor ESCAPE is proceeded by ESCAPE", linenr);
	}
	// escape was followed by quote or escape, go back to quoted state
	goto in_quotes;
carriage_return:
	/* state: carriage_return */
	// this stage optionally skips a newline (\n) character, which allows \r\n to be interpreted as a single line
	if (buffer[position] == '\n') {
		// newline after carriage return: skip
		// increase position by 1 and move start to the new position
		start = ++position;
		if (position >= buffer_size && !ReadBuffer(start)) {
			// file ends right after delimiter, go to final state
			goto final_state;
		}
	}
	if (finished_chunk) {
		return;
	}
	goto value_start;
final_state:
	if (finished_chunk) {
		return;
	}
	if (column > 0 || position > start) {
		// remaining values to be added to the chunk
		AddValue(buffer.get() + start, position - start - offset, column, escape_positions);
		finished_chunk = AddRow(insert_chunk, column);
	}
	// final stage, only reached after parsing the file is finished
	// flush the parsed chunk and finalize parsing
	Flush(insert_chunk);
}

bool BufferedCSVReader::ReadBuffer(idx_t &start) {
	auto old_buffer = move(buffer);

	// the remaining part of the last buffer
	idx_t remaining = buffer_size - start;
	idx_t buffer_read_size = INITIAL_BUFFER_SIZE;
	while (remaining > buffer_read_size) {
		buffer_read_size *= 2;
	}
	if (remaining + buffer_read_size > MAXIMUM_CSV_LINE_SIZE) {
		throw ParserException("Maximum line size of %llu bytes exceeded!", MAXIMUM_CSV_LINE_SIZE);
	}
	buffer = unique_ptr<char[]>(new char[buffer_read_size + remaining + 1]);
	buffer_size = remaining + buffer_read_size;
	if (remaining > 0) {
		// remaining from last buffer: copy it here
		memcpy(buffer.get(), old_buffer.get() + start, remaining);
	}
	source->read(buffer.get() + remaining, buffer_read_size);
	idx_t read_count = source->eof() ? source->gcount() : buffer_read_size;
	buffer_size = remaining + read_count;
	buffer[buffer_size] = '\0';
	if (old_buffer) {
		cached_buffers.push_back(move(old_buffer));
	}
	start = 0;
	position = remaining;

	return read_count > 0;
}

void BufferedCSVReader::ParseCSV(DataChunk &insert_chunk) {
	cached_buffers.clear();

	if (info.quote.size() == 1 && info.escape.size() == 1 && info.delimiter.size() == 1) {
		ParseSimpleCSV(insert_chunk);
	} else {
		ParseComplexCSV(insert_chunk);
	}
}

void BufferedCSVReader::AddValue(char *str_val, idx_t length, idx_t &column, vector<idx_t> &escape_positions) {
	if (column == sql_types.size() && length == 0) {
		// skip a single trailing delimiter
		column++;
		return;
	}
	if (column >= sql_types.size()) {
		throw ParserException("Error on line %lld: expected %lld values but got %d", linenr, sql_types.size(),
		                      column + 1);
	}
	// insert the line number into the chunk
	idx_t row_entry = parse_chunk.size();

	str_val[length] = '\0';
	// test against null string
	if (!info.force_not_null[column] && strcmp(info.null_str.c_str(), str_val) == 0) {
		FlatVector::SetNull(parse_chunk.data[column], row_entry, true);
	} else {
		auto &v = parse_chunk.data[column];
		auto parse_data = FlatVector::GetData<string_t>(v);
		if (escape_positions.size() > 0) {
			// remove escape characters (if any)
			string old_val = str_val;
			string new_val = "";
			idx_t prev_pos = 0;
			for (idx_t i = 0; i < escape_positions.size(); i++) {
				idx_t next_pos = escape_positions[i];
				new_val += old_val.substr(prev_pos, next_pos - prev_pos);
				prev_pos = next_pos + info.escape.size();
			}
			new_val += old_val.substr(prev_pos, old_val.size() - prev_pos);
			escape_positions.clear();
			parse_data[row_entry] = StringVector::AddString(v, new_val.c_str(), new_val.size());
		} else {
			parse_data[row_entry] = string_t(str_val, length);
		}
	}

	// move to the next column
	column++;
}

bool BufferedCSVReader::AddRow(DataChunk &insert_chunk, idx_t &column) {
	if (column < sql_types.size()) {
		throw ParserException("Error on line %lld: expected %lld values but got %d", linenr, sql_types.size(), column);
	}
	parse_chunk.SetCardinality(parse_chunk.size() + 1);
	if (parse_chunk.size() == STANDARD_VECTOR_SIZE) {
		Flush(insert_chunk);
		return true;
	}
	column = 0;
	linenr++;
	return false;
}

void BufferedCSVReader::Flush(DataChunk &insert_chunk) {
	if (parse_chunk.size() == 0) {
		return;
	}
	// convert the columns in the parsed chunk to the types of the table
	insert_chunk.SetCardinality(parse_chunk);
	for (idx_t col_idx = 0; col_idx < sql_types.size(); col_idx++) {
		if (sql_types[col_idx].id == SQLTypeId::VARCHAR) {

			// target type is varchar: no need to convert
			// just test that all strings are valid utf-8 strings
			auto parse_data = FlatVector::GetData<string_t>(parse_chunk.data[col_idx]);
			for (idx_t i = 0; i < parse_chunk.size(); i++) {
				if (!FlatVector::IsNull(parse_chunk.data[col_idx], i)) {
					auto s = parse_data[i];
					auto utf_type = Utf8Proc::Analyze(s.GetData(), s.GetSize());
					switch (utf_type) {
					case UnicodeType::INVALID:
						throw ParserException("Error on line %lld: file is not valid UTF8", linenr);
					case UnicodeType::ASCII:
						break;
					case UnicodeType::UNICODE: {
						auto normie = Utf8Proc::Normalize(s.GetData());
						parse_data[i] = StringVector::AddString(parse_chunk.data[col_idx], normie);
						free(normie);
						break;
					}
					}
				}
			}

			insert_chunk.data[col_idx].Reference(parse_chunk.data[col_idx]);
		} else {
			// target type is not varchar: perform a cast
			VectorOperations::Cast(parse_chunk.data[col_idx], insert_chunk.data[col_idx], SQLType::VARCHAR,
			                       sql_types[col_idx], parse_chunk.size());
		}
	}
	parse_chunk.Reset();
}
