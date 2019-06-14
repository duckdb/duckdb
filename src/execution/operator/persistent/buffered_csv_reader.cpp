#include "execution/operator/persistent/buffered_csv_reader.hpp"
#include "execution/operator/persistent/physical_copy_from_file.hpp"

#include "catalog/catalog_entry/table_catalog_entry.hpp"
#include "common/vector_operations/vector_operations.hpp"
#include "main/database.hpp"
#include "storage/data_table.hpp"
#include "parser/column_definition.hpp"

#include <algorithm>
#include <fstream>

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

void BufferedCSVReader::ParseCSV(DataChunk &insert_chunk) {
	cached_buffers.clear();

	index_t column = 0;
	index_t offset = 0;
	bool in_quotes = false;
	bool finished_chunk = false;

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
		if (in_quotes) {
			if (buffer[position] == info.quote) {
				// end quote
				offset = 1;
				in_quotes = false;
			}
		} else {
			if (buffer[position] == info.quote) {
				// start quotes can only occur at the start of a field
				if (position == start) {
					// increment start by 1
					start++;
					// read until we encounter a quote again
					in_quotes = true;
				}
			} else if (buffer[position] == info.delimiter) {
				// encountered delimiter
				AddValue(buffer.get() + start, position - start - offset, column);
				start = position + 1;
				offset = 0;
			}
			if (is_newline(buffer[position]) || (source.eof() && position + 1 == buffer_size)) {
				char newline = buffer[position];
				// encountered a newline, add the current value and push the row
				AddValue(buffer.get() + start, position - start - offset, column);
				finished_chunk = AddRow(insert_chunk, column);

				// move to the next character
				start = position + 1;
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

		position++;
		if (position >= buffer_size) {
			// exhausted the buffer
			if (!ReadBuffer(start)) {
				break;
			}
		}
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

void BufferedCSVReader::AddValue(char *str_val, index_t length, index_t &column) {
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
	if (length == 0) {
		parse_chunk.data[column].nullmask[row_entry] = true;
	} else {
		auto data = (const char **)parse_chunk.data[column].data;
		data[row_entry] = str_val;
		str_val[length] = '\0';
		if (!Value::IsUTF8String(str_val)) {
			throw ParserException("Error on line %lld: file is not valid UTF8", linenr);
		}
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
			VectorOperations::Cast(parse_chunk.data[col_idx], insert_chunk.data[col_idx], SQLType(SQLTypeId::VARCHAR),
			                       sql_types[col_idx]);
		}
	}
	parse_chunk.Reset();

	nr_elements = 0;
}
