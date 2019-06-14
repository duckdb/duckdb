#include "execution/operator/persistent/physical_copy_to_file.hpp"

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

void PhysicalCopyToFile::GetChunkInternal(ClientContext &context, DataChunk &chunk, PhysicalOperatorState *state) {
	auto &info = *this->info;
	index_t total = 0;
	index_t nr_elements = 0;

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

	chunk.data[0].count = 1;
	chunk.data[0].SetValue(0, Value::BIGINT(total + nr_elements));

	state->finished = true;
}
