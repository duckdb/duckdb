#include "execution/operator/persistent/physical_copy_to_file.hpp"
#include "common/vector_operations/vector_operations.hpp"

#include <algorithm>
#include <fstream>

using namespace duckdb;
using namespace std;

class BufferedWriter {
	constexpr static index_t BUFFER_SIZE = 4096*4;
public:
	BufferedWriter(string &path) :
		pos(0) {
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

	void Write(const char *buf, index_t len) {
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
	index_t pos = 0;

	ofstream to_csv;
};

static void WriteQuotedString(BufferedWriter &writer, const char* str_value, char delimiter, char quote) {
	// scan the string for the delimiter
	bool write_quoted = false;
	index_t len = 0;
	for(const char *val = str_value; *val; val++) {
		len++;
		if (*val == delimiter || *val == '\n' || *val == '\r') {
			// delimiter or newline, write a quoted string
			write_quoted = true;
		}
	}
	if (!write_quoted) {
		writer.Write(str_value, len);
	} else {
		writer.Write(&quote, 1);
		writer.Write(str_value, len);
		writer.Write(&quote, 1);
	}
}

void PhysicalCopyToFile::GetChunkInternal(ClientContext &context, DataChunk &chunk, PhysicalOperatorState *state) {
	auto &info = *this->info;
	index_t total = 0;

	string newline = "\n";
	BufferedWriter writer(info.file_path);
	if (info.header) {
		// write the header line
		for (index_t i = 0; i < names.size(); i++) {
			if (i != 0) {
				writer.Write(&info.delimiter, 1);
			}
			WriteQuotedString(writer, names[i].c_str(), info.delimiter, info.quote);
		}
		writer.Write(newline);
	}
	// cerate a chunk with VARCHAR columns
	vector<TypeId> types;
	for(index_t col_idx = 0; col_idx < state->child_chunk.column_count; col_idx++) {
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
		for(index_t col_idx = 0; col_idx < state->child_chunk.column_count; col_idx++) {
			if (sql_types[col_idx].id == SQLTypeId::VARCHAR) {
				// VARCHAR, just create a reference
				cast_chunk.data[col_idx].Reference(state->child_chunk.data[col_idx]);
			} else {
				// non varchar column, perform the cast
				VectorOperations::Cast(state->child_chunk.data[col_idx], cast_chunk.data[col_idx], sql_types[col_idx], SQLType(SQLTypeId::VARCHAR));
			}
		}
		// now loop over the vectors and output the values
		VectorOperations::Exec(cast_chunk.data[0], [&](index_t i, index_t k) {
			for (index_t col_idx = 0; col_idx < state->child_chunk.column_count; col_idx++) {
				if (col_idx != 0) {
					writer.Write(&info.delimiter, 1);
				}
				if (cast_chunk.data[col_idx].nullmask[i]) {
					continue;
				}
				// non-null value, fetch the string value from the cast chunk
				auto str_value = ((const char**) cast_chunk.data[col_idx].data)[i];
				WriteQuotedString(writer, str_value, info.delimiter, info.quote);
			}
			writer.Write(newline);
		});
		total += cast_chunk.size();
	}
	writer.Close();

	chunk.data[0].count = 1;
	chunk.data[0].SetValue(0, Value::BIGINT(total));

	state->finished = true;
}
