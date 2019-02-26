#include "execution/operator/persistent/physical_copy.hpp"

#include "common/file_system.hpp"
#include "main/client_context.hpp"
#include "storage/data_table.hpp"

#include <algorithm>
#include <fstream>

using namespace duckdb;
using namespace std;

vector<string> split(const string &str, char delimiter, char quote) {
	vector<string> res;
	size_t i = 0;
	if (str[i] == delimiter)
		res.push_back("");
	while (i != str.size()) {
		if (str[i] == quote) {
			i++;
			size_t j = i;
			while (j != str.size() && str[j] != quote)
				j++;
			if (i != j) {
				res.push_back(str.substr(i, j - i));
				i = j;
			}
		} else if (str[i] == delimiter)
			i++;
		size_t j = i;
		while (j != str.size() && str[j] != delimiter)
			j++;
		if (i != j) {
			res.push_back(str.substr(i, j - i));
			i = j;
		} else {
			res.push_back("");
		}
	}
	return res;
}

static bool end_of_field(string &line, size_t i, char delimiter) {
	return i + 1 >= line.size() || line[i] == delimiter;
}

void PhysicalCopy::Flush(ClientContext &context, DataChunk &chunk, int64_t &nr_elements, int64_t &total,
                         vector<bool> &set_to_default) {
	if (nr_elements == 0) {
		return;
	}
	if (set_to_default.size() > 0) {
		assert(set_to_default.size() == chunk.column_count);
		for (size_t i = 0; i < set_to_default.size(); i++) {
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

void PhysicalCopy::_GetChunk(ClientContext &context, DataChunk &chunk, PhysicalOperatorState *state) {
	int64_t nr_elements = 0;
	int64_t total = 0;

	auto &info = *this->info;

	if (table) {
		assert(info.is_from);
		DataChunk insert_chunk;
		auto types = table->GetTypes();
		insert_chunk.Initialize(types);
		// handle the select list (if any)
		vector<size_t> select_list_oid;
		vector<bool> set_to_default;
		if (info.select_list.size() > 0) {
			set_to_default.resize(types.size(), true);
			for (size_t i = 0; i < info.select_list.size(); i++) {
				auto column = table->GetColumn(info.select_list[i]);
				select_list_oid.push_back(column.oid);
				set_to_default[column.oid] = false;
			}
		}
		int64_t linenr = 0;
		string line;
		std::ifstream from_csv;
		from_csv.open(info.file_path);
		if (!FileExists(info.file_path)) {
			throw Exception("File not found");
		}
		if (info.header) {
			// ignore the first line as a header line
			getline(from_csv, line);
			linenr++;
		}
		while (getline(from_csv, line)) {
			bool in_quotes = false;
			size_t start = 0, offset = 0;
			int64_t column = 0;
			int64_t expected_column_count =
			    info.select_list.size() > 0 ? info.select_list.size() : insert_chunk.column_count;
			for (size_t i = 0; i < line.size(); i++) {
				// handle quoting
				if (line[i] == info.quote) {
					if (!in_quotes) {
						// start quotes can only occur at the start of a field
						if (i != start) {
							throw ParserException("Error on line %lld: unexpected quotes in the middle of a field",
							                      linenr);
						}
						// offset start by one
						in_quotes = true;
						start++;
						continue;
					} else {
						// end quotes can only occur at the end of a field
						if (!end_of_field(line, i + 1, info.delimiter)) {
							throw ParserException("Error on line %lld: unexpected quotes in the middle of a field",
							                      linenr);
						}
						// offset end by one
						in_quotes = false;
						offset = 1;
						i++;
					}
				} else if (in_quotes) {
					continue;
				}
				if (end_of_field(line, i, info.delimiter)) {
					assert(i >= start + offset);
					size_t length = i - start - offset;
					if (column == expected_column_count && length == 0) {
						// skip a single trailing delimiter
						column++;
						continue;
					}
					if (column >= expected_column_count) {
						throw ParserException("Error on line %lld: expected %lld values but got %d", linenr,
						                      expected_column_count, column);
					}
					// delimiter, get the value
					Value result;
					if (length > 0) {
						// non-empty: create the value
						result = Value(line.substr(start, length));
					}
					// insert the value into the column
					size_t column_entry = info.select_list.size() > 0 ? select_list_oid[column] : column;
					size_t entry = insert_chunk.data[column_entry].count++;
					insert_chunk.data[column_entry].SetValue(entry, result);
					// move to the next column
					column++;

					start = i + 1;
					offset = 0;
				}
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
		from_csv.close();
	} else {
		ofstream to_csv;
		to_csv.open(info.file_path);
		children[0]->GetChunk(context, state->child_chunk, state->child_state.get());
		if (info.header) {
			throw NotImplementedException("FIXME: write header to file");
		}
		while (state->child_chunk.size() != 0) {
			for (size_t i = 0; i < state->child_chunk.size(); i++) {
				for (size_t col = 0; col < state->child_chunk.column_count; col++) {
					if (col != 0) {
						to_csv << info.delimiter;
					}
					if (state->child_chunk.data[col].type == TypeId::VARCHAR)
						to_csv << info.quote;
					to_csv << state->child_chunk.data[col].GetValue(i).ToString();
					if (state->child_chunk.data[col].type == TypeId::VARCHAR)
						to_csv << info.quote;
				}
				to_csv << endl;
				nr_elements++;
			}
			children[0]->GetChunk(context, state->child_chunk, state->child_state.get());
		}

		to_csv.close();
	}
	chunk.data[0].count = 1;
	chunk.data[0].SetValue(0, Value::BIGINT(total + nr_elements));

	state->finished = true;
}
