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

static bool end_of_field(string &line, index_t i, char delimiter) {
	return i + 1 >= line.size() || line[i] == delimiter;
}

static void WriteQuotedString(ofstream &to_csv, string str, char delimiter, char quote) {
	if (str.find(delimiter) == string::npos) {
		to_csv << str;
	} else {
		to_csv << quote << str << quote;
	}
}

void PhysicalCopy::Flush(ClientContext &context, DataChunk &parse_chunk, DataChunk &insert_chunk, count_t &nr_elements,
                         count_t &total, vector<bool> &set_to_default) {
	if (nr_elements == 0) {
		return;
	}
	// convert the columns in the parsed chunk to the types of the table
	insert_chunk.Reset();
	for (index_t i = 0; i < column_oids.size(); i++) {
		index_t column_idx = column_oids[i];
		if (table->columns[column_idx].type.id == SQLTypeId::VARCHAR) {
			parse_chunk.data[column_idx].Move(insert_chunk.data[column_idx]);
		} else {
			VectorOperations::Cast(parse_chunk.data[column_idx], insert_chunk.data[column_idx],
			                       SQLType(SQLTypeId::VARCHAR), table->columns[column_idx].type);
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
	table->storage->Append(*table, context, insert_chunk);
	nr_elements = 0;
}

void PhysicalCopy::PushValue(string &line, DataChunk &insert_chunk, index_t start, index_t end, index_t &column,
                             index_t linenr) {
	assert(end >= start);
	count_t length = end - start;
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
	index_t row_entry = insert_chunk.data[column_entry].count++;
	if (length == 0) {
		insert_chunk.data[column_entry].nullmask[row_entry] = true;
	}
	auto data = (const char **)insert_chunk.data[column_entry].data;
	data[row_entry] = line.c_str() + start;
	line[start + length] = '\0';
	// move to the next column
	column++;
}

void PhysicalCopy::GetChunkInternal(ClientContext &context, DataChunk &chunk, PhysicalOperatorState *state) {
	count_t nr_elements = 0;
	count_t total = 0;

	auto &info = *this->info;

	if (table) {
		assert(info.is_from);
		DataChunk insert_chunk, parse_chunk;
		// initialize the insert_chunk with the actual to-be-inserted types
		auto types = table->GetTypes();
		insert_chunk.Initialize(types);
		// initialize the parse chunk with VARCHAR data
		for (index_t i = 0; i < types.size(); i++) {
			types[i] = TypeId::VARCHAR;
		}
		parse_chunk.Initialize(types);
		// handle the select list (if any)
		if (info.select_list.size() > 0) {
			set_to_default.resize(types.size(), true);
			for (index_t i = 0; i < info.select_list.size(); i++) {
				auto &column = table->GetColumn(info.select_list[i]);
				column_oids.push_back(column.oid);
				set_to_default[column.oid] = false;
			}
		} else {
			for (index_t i = 0; i < types.size(); i++) {
				column_oids.push_back(i);
			}
		}
		index_t linenr = 0;

		if (!context.db.file_system->FileExists(info.file_path)) {
			throw Exception("File not found");
		}

		unique_ptr<istream> from_csv_stream;
		if (StringUtil::EndsWith(StringUtil::Lower(info.file_path), ".gz")) {
			from_csv_stream = make_unique<GzipStream>(info.file_path);
		} else {
			auto csv_local = make_unique<ifstream>();
			csv_local->open(info.file_path);
			from_csv_stream = move(csv_local);
		}
		vector<unique_ptr<string>> lines;
		string read_line;

		istream &from_csv = *from_csv_stream;

		if (info.header) {
			// ignore the first line as a header line
			getline(from_csv, read_line);
			linenr++;
		}
		auto line_to_read = make_unique<string>();
		while (getline(from_csv, *line_to_read)) {
			auto &line = *line_to_read;
			lines.push_back(move(line_to_read));
			line_to_read = make_unique<string>();

			if (!Value::IsUTF8String(line.c_str())) {
				throw ParserException("Error on line %lld: file is not valid UTF8", linenr);
			}

			bool in_quotes = false;
			index_t start = 0;
			index_t column = 0;
			for (index_t i = 0; i < line.size(); i++) {
				// handle quoting
				if (line[i] == info.quote) {
					if (!in_quotes) {
						// start quotes can only occur at the start of a field
						if (i != start) {
							// quotes in the middle of a line are ignored
							continue;
						}
						// offset start by one
						in_quotes = true;
						start++;
						continue;
					} else {
						if (!end_of_field(line, i + 1, info.delimiter)) {
							// quotes not at the end of a line are ignored
							continue;
						}
						// offset end by one
						in_quotes = false;
						PushValue(line, parse_chunk, start, i, column, linenr);
						start = i + 2;
						i++;
						continue;
					}
				} else if (in_quotes) {
					continue;
				}
				if (line[i] == info.delimiter) {
					PushValue(line, parse_chunk, start, i, column, linenr);
					start = i + 1;
				}
				if (i + 1 >= line.size()) {
					PushValue(line, parse_chunk, start, i + 1, column, linenr);
					break;
				}
			}
			if (in_quotes) {
				throw ParserException("Error on line %lld: unterminated quotes", linenr);
			}
			if (column < column_oids.size()) {
				throw ParserException("Error on line %lld: expected %lld values but got %d", linenr, column_oids.size(),
				                      column);
			}
			nr_elements++;
			if (nr_elements == STANDARD_VECTOR_SIZE) {
				Flush(context, parse_chunk, insert_chunk, nr_elements, total, set_to_default);
				lines.clear();
			}
			linenr++;
		}
		Flush(context, parse_chunk, insert_chunk, nr_elements, total, set_to_default);
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
