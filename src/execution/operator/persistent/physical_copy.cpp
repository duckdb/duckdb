#include "execution/operator/persistent/physical_copy.hpp"

#include "catalog/catalog_entry/table_catalog_entry.hpp"
#include "common/file_system.hpp"
#include "common/gzip_stream.hpp"
#include "main/client_context.hpp"
#include "main/database.hpp"
#include "storage/data_table.hpp"

#include <algorithm>
#include <fstream>

using namespace duckdb;
using namespace std;

static bool end_of_field(string &line, uint64_t i, char delimiter) {
	return i + 1 >= line.size() || line[i] == delimiter;
}

static void WriteQuotedString(ofstream &to_csv, string str, char delimiter, char quote) {
	if (str.find(delimiter) == string::npos) {
		to_csv << str;
	} else {
		to_csv << quote << str << quote;
	}
}

void PhysicalCopy::Flush(ClientContext &context, DataChunk &chunk, int64_t &nr_elements, int64_t &total,
                         vector<bool> &set_to_default) {
	if (nr_elements == 0) {
		return;
	}
	if (set_to_default.size() > 0) {
		assert(set_to_default.size() == chunk.column_count);
		for (uint64_t i = 0; i < set_to_default.size(); i++) {
			if (set_to_default[i]) {
				chunk.data[i].count = nr_elements;
				chunk.data[i].nullmask.set();
			}
		}
	}
	total += nr_elements;
	table->storage->Append(*table, context, chunk);
	chunk.Reset();
	nr_elements = 0;
}

void PhysicalCopy::PushValue(string &line, DataChunk &insert_chunk, int64_t start, int64_t end, int64_t &column,
                             int64_t linenr) {
	assert(end >= start);
	int64_t expected_column_count = info->select_list.size() > 0 ? info->select_list.size() : insert_chunk.column_count;
	uint64_t length = end - start;
	if (column == expected_column_count && length == 0) {
		// skip a single trailing delimiter
		column++;
		return;
	}
	if (column >= expected_column_count) {
		throw ParserException("Error on line %lld: expected %lld values but got %d", linenr, expected_column_count,
		                      column + 1);
	}
	// delimiter, get the value
	Value result;
	if (length > 0) {
		// non-empty: create the value
		result = Value(line.substr(start, length));
	}
	// insert the value into the column
	uint64_t column_entry = info->select_list.size() > 0 ? select_list_oid[column] : column;
	uint64_t entry = insert_chunk.data[column_entry].count++;
	insert_chunk.data[column_entry].SetValue(entry, result);
	// move to the next column
	column++;
}

void PhysicalCopy::GetChunkInternal(ClientContext &context, DataChunk &chunk, PhysicalOperatorState *state) {
	int64_t nr_elements = 0;
	int64_t total = 0;

	auto &info = *this->info;

	if (table) {
		assert(info.is_from);
		DataChunk insert_chunk;
		auto types = table->GetTypes();
		insert_chunk.Initialize(types);
		// handle the select list (if any)
		if (info.select_list.size() > 0) {
			set_to_default.resize(types.size(), true);
			for (uint64_t i = 0; i < info.select_list.size(); i++) {
				auto &column = table->GetColumn(info.select_list[i]);
				select_list_oid.push_back(column.oid);
				set_to_default[column.oid] = false;
			}
		}
		int64_t linenr = 0;
		string line;

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

		istream &from_csv = *from_csv_stream;

		if (info.header) {
			// ignore the first line as a header line
			getline(from_csv, line);
			linenr++;
		}
		while (getline(from_csv, line)) {
			bool in_quotes = false;
			uint64_t start = 0;
			int64_t column = 0;
			int64_t expected_column_count =
			    info.select_list.size() > 0 ? info.select_list.size() : insert_chunk.column_count;
			for (uint64_t i = 0; i < line.size(); i++) {
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
						PushValue(line, insert_chunk, start, i, column, linenr);
						start = i + 2;
						i++;
						continue;
					}
				} else if (in_quotes) {
					continue;
				}
				if (line[i] == info.delimiter) {
					PushValue(line, insert_chunk, start, i, column, linenr);
					start = i + 1;
				}
				if (i + 1 >= line.size()) {
					PushValue(line, insert_chunk, start, i + 1, column, linenr);
					break;
				}
			}
			if (in_quotes) {
				throw ParserException("Error on line %lld: unterminated quotes", linenr);
			}
			if (column < expected_column_count) {
				throw ParserException("Error on line %lld: expected %lld values but got %d", linenr,
				                      expected_column_count, column);
			}
			nr_elements++;
			if (nr_elements == STANDARD_VECTOR_SIZE) {
				Flush(context, insert_chunk, nr_elements, total, set_to_default);
			}
			linenr++;
		}
		Flush(context, insert_chunk, nr_elements, total, set_to_default);
	} else {
		ofstream to_csv;
		to_csv.open(info.file_path);
		if (info.header) {
			// write the header line
			for (uint64_t i = 0; i < names.size(); i++) {
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
			for (uint64_t i = 0; i < state->child_chunk.size(); i++) {
				for (uint64_t col = 0; col < state->child_chunk.column_count; col++) {
					if (col != 0) {
						to_csv << info.delimiter;
					}
					WriteQuotedString(to_csv, state->child_chunk.data[col].GetValue(i).ToString(), info.delimiter,
					                  info.quote);
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
