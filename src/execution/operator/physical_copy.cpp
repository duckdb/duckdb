#include "execution/operator/physical_copy.hpp"
#include "common/string_util.hpp"
#include "storage/data_table.hpp"

using namespace duckdb;
using namespace std;

std::vector<TypeId> PhysicalCopy::GetTypes() { return {TypeId::INTEGER}; }

void PhysicalCopy::GetChunk(DataChunk &result_chunk,
                            PhysicalOperatorState *state_) {
	result_chunk.Reset();
	if (state_->finished) {
		return;
	}
	DataChunk insert_chunk;
	vector<TypeId> types;
	for (auto &column : table->columns) {
		types.push_back(column->type);
	}
	insert_chunk.Initialize(types);

	int count_line = 0;
	int stored_chunks = 0;

	if (is_from) {
		string value;
		std::ifstream from_csv;
		from_csv.open(file_path);
		while (getline(from_csv, value)) {
			if (count_line == insert_chunk.maximum_size) {
				insert_chunk.count = insert_chunk.data[0]->count;
				table->storage->AddData(insert_chunk);
				count_line = 0;
				stored_chunks++;
				insert_chunk.Reset();
			}
			std::vector<string> csv_line = StringUtil::Split(value, delimiter);
			if (csv_line.size() != insert_chunk.column_count) {
				throw Exception(
				    "COPY TO column mismatch! Expected %zu columns, got %zu.",
				    insert_chunk.column_count, csv_line.size());
			}
			for (size_t i = 0; i < csv_line.size(); ++i) {
				insert_chunk.data[i]->count++;
				insert_chunk.data[i]->SetValue(count_line, csv_line[i]);
			}
			count_line++;
		}
		insert_chunk.count = insert_chunk.data[0]->count;
		table->storage->AddData(insert_chunk);
		from_csv.close();
	} else {
		std::ofstream to_csv;
		to_csv.open(file_path);
		size_t current_offset = 0;
		while (current_offset <
		       (table->storage->size + insert_chunk.maximum_size - 1) /
		           insert_chunk.maximum_size) {
			for (size_t chunk_id = 0; chunk_id < table->storage->columns.size();
			     ++chunk_id) {
				auto column = table->storage->columns[chunk_id].get();
				auto &v = column->data[current_offset];
				insert_chunk.data[chunk_id]->data = v->data;
				insert_chunk.data[chunk_id]->owns_data = false;
				insert_chunk.data[chunk_id]->count = v->count;
			}
			insert_chunk.count = insert_chunk.data[0]->count;
			for (size_t chunk_tuple_id = 0; chunk_tuple_id < insert_chunk.count;
			     chunk_tuple_id++) {
				for (size_t column_id = 0;
				     column_id < insert_chunk.column_count; column_id++) {
					if (column_id == 0)
						to_csv << insert_chunk.data[column_id]
						              ->GetValue(chunk_tuple_id)
						              .ToString();
					else
						to_csv << delimiter
						       << insert_chunk.data[column_id]
						              ->GetValue(chunk_tuple_id)
						              .ToString();
				}
				to_csv << endl;
				count_line++;
			}
			current_offset++;
			insert_chunk.Reset();
		}
		to_csv.close();
	}
	result_chunk.data[0]->count = 1;
	result_chunk.data[0]->SetValue(
	    0,
	    Value::NumericValue(TypeId::INTEGER,
	                        stored_chunks * result_chunk.data[0]->maximum_size +
	                            count_line));
	result_chunk.count = 1;
	state_->finished = true;
}

unique_ptr<PhysicalOperatorState>
PhysicalCopy::GetOperatorState(ExpressionExecutor *executor) {
	return make_unique<PhysicalOperatorState>(nullptr, executor);
}
