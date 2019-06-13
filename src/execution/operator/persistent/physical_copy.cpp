#include "execution/operator/persistent/physical_copy.hpp"

#include "catalog/catalog_entry/table_catalog_entry.hpp"
#include "common/vector_operations/vector_operations.hpp"
#include "common/file_system.hpp"
#include "common/gzip_stream.hpp"
#include "main/client_context.hpp"
#include "main/database.hpp"
#include "storage/data_table.hpp"
#include "parser/column_definition.hpp"

#include <algorithm>
#include <fstream>

using namespace duckdb;
using namespace std;

static void WriteQuotedString(ofstream &to_csv, string str, char delimiter, char quote) {
	if (str.find(delimiter) == string::npos) {
		to_csv << str;
	} else {
		to_csv << quote << str << quote;
	}
}

static char is_newline(char c) {
	return c == '\n' || c == '\r';
}

BufferedCSVReader::BufferedCSVReader(PhysicalCopy &copy, istream &source) :
	copy(copy), source(source), buffer_size(0), position(0) {}

void BufferedCSVReader::AddValue(char *str_val, index_t length, index_t &column) {
	if (column == column_oids.size() && length == 0) {
		// skip a single trailing delimiter
		column++;
		return;
	}
	if (column >= column_oids.size()) {
		throw ParserException("Error on line %lld: expected %lld values but got %d", linenr, column_oids.size(),
		                      column + 1);
	}
	// insert the line number into the chunk
	index_t column_entry = column_oids[column];
	index_t row_entry = parse_chunk.data[column_entry].count++;
	if (length == 0) {
		parse_chunk.data[column_entry].nullmask[row_entry] = true;
	} else {
		auto data = (const char **)parse_chunk.data[column_entry].data;
		data[row_entry] = str_val;
		str_val[length] = '\0';
		if (!Value::IsUTF8String(str_val)) {
			throw ParserException("Error on line %lld: file is not valid UTF8", linenr);
		}
	}
	// move to the next column
	column++;
}

void BufferedCSVReader::AddRow(ClientContext &context, index_t &column) {
	if (column < column_oids.size()) {
		throw ParserException("Error on line %lld: expected %lld values but got %d", linenr, column_oids.size(),
								column);
	}
	nr_elements++;
	if (nr_elements == STANDARD_VECTOR_SIZE) {
		Flush(context);
		cached_buffers.clear();
	}
	column = 0;
	linenr++;
}

void BufferedCSVReader::Flush(ClientContext &context) {
	if (nr_elements == 0) {
		return;
	}
	// convert the columns in the parsed chunk to the types of the table
	insert_chunk.Reset();
	for (index_t i = 0; i < column_oids.size(); i++) {
		index_t column_idx = column_oids[i];
		if (copy.table->columns[column_idx].type.id == SQLTypeId::VARCHAR) {
			parse_chunk.data[column_idx].Move(insert_chunk.data[column_idx]);
		} else {
			VectorOperations::Cast(parse_chunk.data[column_idx], insert_chunk.data[column_idx],
			                       SQLType(SQLTypeId::VARCHAR), copy.table->columns[column_idx].type);
		}
	}
	parse_chunk.Reset();

	if (set_to_default.size() > 0) {
		assert(set_to_default.size() == insert_chunk.column_count);
		for (index_t i = 0; i < set_to_default.size(); i++) {
			if (set_to_default[i]) {
				insert_chunk.data[i].count = nr_elements;
				insert_chunk.data[i].nullmask.set();
			}
		}
	}
	// now insert the chunk into the storage
	total += nr_elements;
	copy.table->storage->Append(*copy.table, context, insert_chunk);
	nr_elements = 0;
}

void BufferedCSVReader::ParseCSV(ClientContext &context) {
	auto &info = *copy.info;
	if (info.header) {
		// ignore the first line as a header line
		string read_line;
		getline(source, read_line);
		linenr++;
	}

	index_t start = 0;
	index_t column = 0;
	index_t offset = 0;
	bool in_quotes = false;

	if (position >= buffer_size) {
		if (!ReadBuffer(start)) {
			return;
		}
	}

	// read until we exhaust the stream
	while(true) {
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
				AddRow(context, column);
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
	Flush(context);
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

void PhysicalCopy::GetChunkInternal(ClientContext &context, DataChunk &chunk, PhysicalOperatorState *state) {
	auto &info = *this->info;
	index_t total = 0;
	index_t nr_elements = 0;

	if (table) {
		assert(info.is_from);
		if (!context.db.file_system->FileExists(info.file_path)) {
			throw Exception("File not found");
		}

		unique_ptr<istream> csv_stream;
		if (StringUtil::EndsWith(StringUtil::Lower(info.file_path), ".gz")) {
			csv_stream = make_unique<GzipStream>(info.file_path);
		} else {
			auto csv_local = make_unique<ifstream>();
			csv_local->open(info.file_path);
			csv_stream = move(csv_local);
		}

		BufferedCSVReader reader(*this, *csv_stream);
		// initialize the insert_chunk with the actual to-be-inserted types
		auto types = table->GetTypes();
		reader.insert_chunk.Initialize(types);
		// initialize the parse chunk with VARCHAR data
		for (index_t i = 0; i < types.size(); i++) {
			types[i] = TypeId::VARCHAR;
		}
		reader.parse_chunk.Initialize(types);
		// handle the select list (if any)
		if (info.select_list.size() > 0) {
			reader.set_to_default.resize(types.size(), true);
			for (index_t i = 0; i < info.select_list.size(); i++) {
				auto &column = table->GetColumn(info.select_list[i]);
				reader.column_oids.push_back(column.oid);
				reader.set_to_default[column.oid] = false;
			}
		} else {
			for (index_t i = 0; i < types.size(); i++) {
				reader.column_oids.push_back(i);
			}
		}
		reader.ParseCSV(context);
		total = reader.total + reader.nr_elements;
	} else {
		ofstream to_csv;
		to_csv.open(info.file_path);
		if (info.header) {
			// write the header line
			for (index_t i = 0; i < names.size(); i++) {
				if (i != 0) {
					to_csv << info.delimiter;
				}
				WriteQuotedString(to_csv, names[i], info.delimiter, info.quote);
			}
			to_csv << endl;
		}
		while (true) {
			children[0]->GetChunk(context, state->child_chunk, state->child_state.get());
			if (state->child_chunk.size() == 0) {
				break;
			}
			for (index_t i = 0; i < state->child_chunk.size(); i++) {
				for (index_t col = 0; col < state->child_chunk.column_count; col++) {
					if (col != 0) {
						to_csv << info.delimiter;
					}
					// need to cast to correct sql type because otherwise the string representation is wrong
					auto val = state->child_chunk.data[col].GetValue(i);
					WriteQuotedString(to_csv, val.ToString(sql_types[col]), info.delimiter, info.quote);
				}
				to_csv << endl;
				nr_elements++;
			}
		}
		to_csv.close();
	}
	chunk.data[0].count = 1;
	chunk.data[0].SetValue(0, Value::BIGINT(total + nr_elements));

	state->finished = true;
}
