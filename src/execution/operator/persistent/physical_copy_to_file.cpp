#include "duckdb/execution/operator/persistent/physical_copy_to_file.hpp"
#include "duckdb/common/vector_operations/vector_operations.hpp"

#include <algorithm>
#include <fstream>

using namespace duckdb;
using namespace std;

class BufferedWriter {
	constexpr static idx_t BUFFER_SIZE = 4096 * 4;

public:
	BufferedWriter(string &path) : pos(0) {
		to_csv.open(path);
		if (to_csv.fail()) {
			throw IOException("Could not open CSV file");
		}
	}

	void Flush() {
		if (pos > 0) {
			to_csv.write(buffer, pos);
			pos = 0;
		}
	}

	void Close() {
		Flush();
		to_csv.close();
	}

	void Write(const char *buf, idx_t len) {
		if (len >= BUFFER_SIZE) {
			Flush();
			to_csv.write(buf, len);
			return;
		}
		if (pos + len > BUFFER_SIZE) {
			Flush();
		}
		memcpy(buffer + pos, buf, len);
		pos += len;
	}

	void Write(string &value) {
		Write(value.c_str(), value.size());
	}

private:
	char buffer[BUFFER_SIZE];
	idx_t pos = 0;

	ofstream to_csv;
};

string AddEscapes(string &to_be_escaped, string escape, string val) {
	idx_t i = 0;
	string new_val = "";
	idx_t found = val.find(to_be_escaped);

	while (found != string::npos) {
		while (i < found) {
			new_val += val[i];
			i++;
		}
		new_val += escape;
		found = val.find(to_be_escaped, found + escape.length());
	}
	while (i < val.length()) {
		new_val += val[i];
		i++;
	}
	return new_val;
}

static void WriteQuotedString(BufferedWriter &writer, string_t str_value, string &delimiter, string &quote,
                              string &escape, string &null_str, bool write_quoted) {
	// used for adding escapes
	bool add_escapes = false;
	auto str_data = str_value.GetData();
	string new_val(str_data, str_value.GetSize());

	// check for \n, \r, \n\r in string
	if (!write_quoted) {
		for (idx_t i = 0; i < str_value.GetSize(); i++) {
			if (str_data[i] == '\n' || str_data[i] == '\r') {
				// newline, write a quoted string
				write_quoted = true;
			}
		}
	}

	// check if value is null string
	if (!write_quoted) {
		if (new_val == null_str) {
			write_quoted = true;
		}
	}

	// check for delimiter
	if (!write_quoted) {
		if (new_val.find(delimiter) != string::npos) {
			write_quoted = true;
		}
	}

	// check for quote
	if (new_val.find(quote) != string::npos) {
		write_quoted = true;
		add_escapes = true;
	}

	// check for escapes in quoted string
	if (write_quoted && !add_escapes) {
		if (new_val.find(escape) != string::npos) {
			add_escapes = true;
		}
	}

	if (add_escapes) {
		new_val = AddEscapes(escape, escape, new_val);
		// also escape quotes
		if (escape != quote) {
			new_val = AddEscapes(quote, escape, new_val);
		}
	}

	if (!write_quoted) {
		writer.Write(new_val);
	} else {
		writer.Write(quote);
		writer.Write(new_val);
		writer.Write(quote);
	}
}

void PhysicalCopyToFile::GetChunkInternal(ClientContext &context, DataChunk &chunk, PhysicalOperatorState *state) {
	auto &info = *this->info;
	idx_t total = 0;

	string newline = "\n";
	BufferedWriter writer(info.file_path);
	if (info.header) {
		// write the header line
		for (idx_t i = 0; i < names.size(); i++) {
			if (i != 0) {
				writer.Write(info.delimiter);
			}
			WriteQuotedString(writer, names[i].c_str(), info.delimiter, info.quote, info.escape, info.null_str, false);
		}
		writer.Write(newline);
	}
	// create a chunk with VARCHAR columns
	vector<TypeId> types;
	for (idx_t col_idx = 0; col_idx < state->child_chunk.column_count(); col_idx++) {
		types.push_back(TypeId::VARCHAR);
	}
	DataChunk cast_chunk;
	cast_chunk.Initialize(types);

	while (true) {
		children[0]->GetChunk(context, state->child_chunk, state->child_state.get());
		if (state->child_chunk.size() == 0) {
			break;
		}
		// cast the columns of the chunk to varchar
		cast_chunk.SetCardinality(state->child_chunk);
		for (idx_t col_idx = 0; col_idx < state->child_chunk.column_count(); col_idx++) {
			if (sql_types[col_idx].id == SQLTypeId::VARCHAR || sql_types[col_idx].id == SQLTypeId::BLOB) {
				// VARCHAR, just create a reference
				cast_chunk.data[col_idx].Reference(state->child_chunk.data[col_idx]);
			} else {
				// non varchar column, perform the cast
				VectorOperations::Cast(state->child_chunk.data[col_idx], cast_chunk.data[col_idx], sql_types[col_idx],
				                       SQLType::VARCHAR, cast_chunk.size());
			}
		}
		cast_chunk.Normalify();
		// now loop over the vectors and output the values
		for (idx_t i = 0; i < cast_chunk.size(); i++) {
			// write values
			for (idx_t col_idx = 0; col_idx < state->child_chunk.column_count(); col_idx++) {
				if (col_idx != 0) {
					writer.Write(info.delimiter);
				}
				if (FlatVector::IsNull(cast_chunk.data[col_idx], i)) {
					// write null value
					writer.Write(info.null_str);
					continue;
				}

				// non-null value, fetch the string value from the cast chunk
				auto str_data = FlatVector::GetData<string_t>(cast_chunk.data[col_idx]);
				auto str_value = str_data[i];
				WriteQuotedString(writer, str_value, info.delimiter, info.quote, info.escape, info.null_str,
				                  info.force_quote[col_idx]);
			}
			writer.Write(newline);
		}
		total += cast_chunk.size();
	}
	writer.Close();

	chunk.SetCardinality(1);
	chunk.SetValue(0, 0, Value::BIGINT(total));

	state->finished = true;
}
