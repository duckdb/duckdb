#include "execution/operator/physical_copy.hpp"
#include "storage/data_table.hpp"
#include <algorithm>

using namespace duckdb;
using namespace std;

std::vector<TypeId> PhysicalCopy::GetTypes() { return {TypeId::BIGINT}; }

vector<string> split(const string &str, char delimiter, char quote) {
	vector<string> res;
	size_t i = 0;
	while (i != str.size()) {
		if (i != str.size() && str[i] == quote) {
			i++;
			size_t j = i;
			while (j != str.size() && str[j] != quote)
				j++;
			if (i != j) {
				res.push_back(str.substr(i, j - i));
				i = j;
			}
		} else if (i != str.size() && str[i] == delimiter)
			i++;
		size_t j = i;
		while (j != str.size() && str[j] != delimiter)
			j++;
		if (i != j) {
			res.push_back(str.substr(i, j - i));
			i = j;
		}
	}
	return res;
}

void PhysicalCopy::GetChunk(ClientContext &context, DataChunk &chunk, PhysicalOperatorState *state) {
	chunk.Reset();
	if (state->finished) {
		return;
	}
	int64_t count_line = 0;
	int64_t total = 0;

	if (table) {
		assert(is_from);
		DataChunk insert_chunk;
		auto types = table->GetTypes();
		insert_chunk.Initialize(types);
		std::vector<size_t> select_list_oid;
		for (size_t i = 0; i < select_list.size(); i++) {
			auto column = table->GetColumn(select_list[i]);
			select_list_oid.push_back(column->oid);
		}
		string value;
		std::ifstream from_csv;
		from_csv.open(file_path);
		while (getline(from_csv, value)) {
			if (count_line == STANDARD_VECTOR_SIZE) {
				insert_chunk.count = insert_chunk.data[0].count;
				table->storage->AddData(insert_chunk);
				total += count_line;
				count_line = 0;
				insert_chunk.Reset();
			}
			vector<string> csv_line = split(value, delimiter, quote);

			if (select_list.size()) {
				for (size_t i = 0; i < table->columns.size(); ++i) {
					if (!(std::find(select_list_oid.begin(),
					                select_list_oid.end(),
					                i) != select_list_oid.end())) {
						insert_chunk.data[i].count++;
						insert_chunk.data[i].SetValue(count_line, Value());
					}
				}
				for (size_t i = 0; i < csv_line.size(); ++i) {
					insert_chunk.data[select_list_oid[i]].count++;
					insert_chunk.data[select_list_oid[i]].SetValue(count_line,
					                                               csv_line[i]);
				}
			} else {
				for (size_t i = 0; i < csv_line.size(); ++i) {
					insert_chunk.data[i].count++;
					insert_chunk.data[i].SetValue(count_line, csv_line[i]);
				}
				for (size_t i = csv_line.size(); i < table->columns.size();
				     ++i) {
					insert_chunk.data[i].count++;
					insert_chunk.data[i].SetValue(count_line, Value());
				}
			}

			count_line++;
		}
		insert_chunk.count = insert_chunk.data[0].count;
		table->storage->AddData(insert_chunk);
		from_csv.close();
	} else {
		ofstream to_csv;
		to_csv.open(file_path);
		children[0]->GetChunk(context, state->child_chunk, state->child_state.get());
		while (state->child_chunk.count != 0) {
			for (size_t i = 0; i < state->child_chunk.count; i++) {
				for (size_t col = 0; col < state->child_chunk.column_count;
				     col++) {
					if (col != 0) {
						to_csv << delimiter;
					}
					if (state->child_chunk.data[col].type == TypeId::VARCHAR)
						to_csv << quote;
					to_csv
					    << state->child_chunk.data[col].GetValue(i).ToString();
					if (state->child_chunk.data[col].type == TypeId::VARCHAR)
						to_csv << quote;
				}
				to_csv << endl;
				count_line++;
			}
			children[0]->GetChunk(context, state->child_chunk, state->child_state.get());
		}

		to_csv.close();
	}
	chunk.data[0].count = 1;
	chunk.data[0].SetValue(0, Value::BIGINT(total + count_line));
	chunk.count = 1;

	state->finished = true;

	chunk.Verify();
}

unique_ptr<PhysicalOperatorState>
PhysicalCopy::GetOperatorState(ExpressionExecutor *executor) {
	return make_unique<PhysicalOperatorState>(
	    children.size() == 0 ? nullptr : children[0].get(), executor);
}
