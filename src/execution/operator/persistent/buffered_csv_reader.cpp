#include "execution/operator/persistent/buffered_csv_reader.hpp"
#include "execution/operator/persistent/physical_copy_from_file.hpp"

#include "catalog/catalog_entry/table_catalog_entry.hpp"
#include "common/vector_operations/vector_operations.hpp"
#include "main/database.hpp"
#include "storage/data_table.hpp"
#include "parser/column_definition.hpp"

#include <algorithm>
#include <fstream>
#include <queue>

using namespace duckdb;
using namespace std;

static char is_newline(char c) {
	return c == '\n' || c == '\r';
}

BufferedCSVReader::BufferedCSVReader(CopyInfo &info, vector<SQLType> sql_types, istream &source)
    : info(info), sql_types(sql_types), source(source), buffer_size(0), position(0), start(0) {
	// initialize the parse_chunk with a set of VARCHAR types
	vector<TypeId> varchar_types;
	for (index_t i = 0; i < sql_types.size(); i++) {
		varchar_types.push_back(TypeId::VARCHAR);
	}
	parse_chunk.Initialize(varchar_types);

	if (info.header) {
		// ignore the first line as a header line
		string read_line;
		getline(source, read_line);
		linenr++;
	}
}

bool BufferedCSVReader::MatchControlString(bool &delim_match, bool &quote_match, bool &escape_match) {
	index_t tmp_position = position;
	index_t control_string_offset = 0;

	bool delim = true;
	bool quote = true;
	bool escape = true;

	while (true) {
		// check if the delimiter string matches
		if (delim && control_string_offset < info.delimiter.length()) {
			if (buffer[tmp_position] != info.delimiter[control_string_offset]) {
				delim = false;
			} else {
				if (control_string_offset == info.delimiter.length() - 1) {
					delim = false;
					delim_match = true;
				}
			}
		}

		// check if the quote string matches
		if (quote && control_string_offset < info.quote.length()) {
			if (buffer[tmp_position] != info.quote[control_string_offset]) {
				quote = false;
			} else {
				if (control_string_offset == info.quote.length() - 1) {
					quote = false;
					quote_match = true;
				}
			}
		}

		// check if the escape string matches
		if (escape && control_string_offset < info.escape.length()) {
			if (buffer[tmp_position] != info.escape[control_string_offset]) {
				escape = false;
			} else {
				if (control_string_offset == info.escape.length() - 1) {
					escape = false;
					escape_match = true;
				}
			}
		}

		// return if matching is not possible any longer
		if (!delim && !quote && !escape) {
			return false;
		}

		tmp_position++;
		control_string_offset++;

		// make sure not to exceed buffer size, and return if there cannot be any further control strings
		if (tmp_position >= buffer_size) {
			return true;
		}
	}
}

void BufferedCSVReader::ParseCSV(DataChunk &insert_chunk) {
	cached_buffers.clear();

	// used for parsing algorithm
	bool in_quotes = false;
	bool finished_chunk = false;
	bool seen_escape = false;
	bool reset_quotes = false;
	bool quote_or_escape = false;
	bool exhausted_buffer = false;
	index_t column = 0;
	index_t offset = 0;
	std::queue<index_t> escape_positions;

	// used for fast control sequence detection
	bool delimiter = false;
	bool quote = false;
	bool escape = false;

	if (position >= buffer_size) {
		if (!ReadBuffer(start)) {
			return;
		}
	}

	// read until we exhaust the stream
	while (true) {
		if (finished_chunk) {
			return;
		}

		// detect control strings
		exhausted_buffer = MatchControlString(delimiter, quote, escape);

		if (!exhausted_buffer) {
			// if QUOTE equals ESCAPE we might need to determine which one we detected in the previous loop
			if (quote_or_escape) {
				if (delimiter || is_newline(buffer[position]) || (source.eof() && position + 1 == buffer_size)) {
					// found quote without escape, end quote
					offset = info.quote.length();
					in_quotes = false;
				} else {
					// found escape
					seen_escape = true;
				}
				quote_or_escape = false;
			}

			if (in_quotes) {
				if (!quote && !escape && !seen_escape) {
					// plain value character
					seen_escape = false;
				} else if (!quote && !escape && seen_escape) {
					throw ParserException("Error on line %lld: neither QUOTE nor ESCAPE is proceeded by ESCAPE", linenr);
				} else if (!quote && escape && !seen_escape) {
					// escape
					seen_escape = true;
					position += info.escape.length() - 1;
				} else if (!quote && escape && seen_escape) {
					// escaped escape
					// we store the position of the escape so we can skip it when adding the value
					escape_positions.push(position);
					position += info.escape.length() - 1;
					seen_escape = false;
				} else if (quote && !escape && !seen_escape) {
					// found quote without escape, end quote
					offset = info.quote.length();
					position += info.quote.length() - 1;
					in_quotes = false;
				} else if (quote && !escape && seen_escape) {
					// escaped quote
					// we store the position of the escape so we can skip it when adding the value
					escape_positions.push(position);
					position += info.quote.length() - 1;
					seen_escape = false;
				} else if (quote && escape && !seen_escape) {
					// either escape or end of quote, decide depending on next character
					// NOTE: QUOTE and ESCAPE cannot be subsets of each other
					position += info.escape.length() - 1;
					quote_or_escape = true;
				} else if (quote && escape && seen_escape) {
					// we store the position of the escape so we can skip it when adding the value
					escape_positions.push(position);
					position += info.escape.length() - 1;
					seen_escape = false;
				}
			} else {
				if (quote) {
					// start quotes can only occur at the start of a field
					if (position == start) {
						in_quotes = true;
						// increment start by quote length
						start += info.quote.length();
						reset_quotes = in_quotes;
						position += info.quote.length() - 1;
					} else {
						throw ParserException("Error on line %lld: unterminated quotes", linenr);
					}
				} else if (delimiter) {
					// encountered delimiter
					AddValue(buffer.get() + start, position - start - offset, column, escape_positions);
					start = position + info.delimiter.length();
					reset_quotes = in_quotes;
					position += info.delimiter.length() - 1;
					offset = 0;
				}

				if (is_newline(buffer[position]) || (source.eof() && position + 1 == buffer_size)) {
					char newline = buffer[position];
					// encountered a newline, add the current value and push the row
					AddValue(buffer.get() + start, position - start - offset, column, escape_positions);
					finished_chunk = AddRow(insert_chunk, column);

					// move to the next character
					start = position + 1;
					reset_quotes = in_quotes;
					offset = 0;
					if (newline == '\r') {
						// \r, skip subsequent \n
						if (position + 1 >= buffer_size) {
							if (!ReadBuffer(start)) {
								break;
							}
							if (buffer[position] == '\n') {
								start++;
								position++;
							}
							continue;
						}
						if (buffer[position + 1] == '\n') {
							start++;
							position++;
						}
					}
				}
				if (offset != 0) {
					in_quotes = true;
				}
			}
		}

		position++;
		if (position >= buffer_size) {
			// exhausted the buffer
			if (!ReadBuffer(start)) {
				break;
			}
			// restore the current state after reading from the buffer
			in_quotes = reset_quotes;
			seen_escape = false;
			position = start;
			quote_or_escape = false;
			while (!escape_positions.empty()) {
				escape_positions.pop();
			}
		}

		// reset values for control string matching
		delimiter = false;
		quote = false;
		escape = false;
	}

	if (in_quotes) {
		throw ParserException("Error on line %lld: unterminated quotes", linenr);
	}
	Flush(insert_chunk);
}

bool BufferedCSVReader::ReadBuffer(index_t &start) {
	auto old_buffer = move(buffer);

	// the remaining part of the last buffer
	index_t remaining = buffer_size - start;
	index_t buffer_read_size = INITIAL_BUFFER_SIZE;
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
	source.read(buffer.get() + remaining, buffer_read_size);
	index_t read_count = source.eof() ? source.gcount() : buffer_read_size;
	buffer_size = remaining + read_count;
	buffer[buffer_size] = '\0';
	if (old_buffer && start != 0) {
		cached_buffers.push_back(move(old_buffer));
	}
	start = 0;
	position = remaining;

	return read_count > 0;
}

void BufferedCSVReader::AddValue(char *str_val, index_t length, index_t &column, std::queue<index_t> &escape_positions) {
	// used to remove escape characters
	index_t pos = start;
	bool in_escape = false;

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
	index_t row_entry = parse_chunk.data[column].count++;

	// test against null string
	str_val[length] = '\0';
	if (info.null_str == str_val && !info.force_not_null[column]) {
		parse_chunk.data[column].nullmask[row_entry] = true;
	} else {
		// remove escape(s)
		if (!escape_positions.empty()) {
			string new_val = "";
			for (const char *val = str_val; *val; val++) {
				if (escape_positions.front() == pos) {
					in_escape = false;
					escape_positions.pop();
				}
				if (escape_positions.front() - info.escape.length() == pos) {
					in_escape = true;
				}
				if (!in_escape) {
					new_val += *val;
				}
				pos++;
			}
			strcpy(str_val, new_val.c_str());
			while (!escape_positions.empty()) {
				escape_positions.pop();
			}
		}

		// test for valid utf-8 string
		if (!Value::IsUTF8String(str_val)) {
			throw ParserException("Error on line %lld: file is not valid UTF8", linenr);
		}
		auto data = (const char **)parse_chunk.data[column].data;
		data[row_entry] = str_val;
	}

	// move to the next column
	column++;
}

bool BufferedCSVReader::AddRow(DataChunk &insert_chunk, index_t &column) {
	if (column < sql_types.size()) {
		throw ParserException("Error on line %lld: expected %lld values but got %d", linenr, sql_types.size(), column);
	}
	nr_elements++;
	if (nr_elements == STANDARD_VECTOR_SIZE) {
		Flush(insert_chunk);
		return true;
	}
	column = 0;
	linenr++;
	return false;
}

void BufferedCSVReader::Flush(DataChunk &insert_chunk) {
	if (nr_elements == 0) {
		return;
	}
	// convert the columns in the parsed chunk to the types of the table
	for (index_t col_idx = 0; col_idx < sql_types.size(); col_idx++) {
		if (sql_types[col_idx].id == SQLTypeId::VARCHAR) {
			// target type is varchar: just move the parsed chunk
			parse_chunk.data[col_idx].Move(insert_chunk.data[col_idx]);
		} else {
			// target type is not varchar: perform a cast
			VectorOperations::Cast(parse_chunk.data[col_idx], insert_chunk.data[col_idx], SQLType::VARCHAR,
			                       sql_types[col_idx]);
		}
	}
	parse_chunk.Reset();

	nr_elements = 0;
}
