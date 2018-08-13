#include "execution/operator/physical_copy.hpp"
#include "storage/data_table.hpp"

using namespace duckdb;
using namespace std;

std::vector<TypeId> PhysicalCopy::GetTypes() { return {TypeId::BIGINT}; }

vector<string> split (const string & str,  char delimiter, char quote){
	vector<string> res;
	size_t i = 0;
	while (i != str.size()){
        if (i!= str.size() && str[i] == quote ){
            i++;
            size_t j = i;
            while (j != str.size() && str[j] != quote )
                j++;
            if (i!=j){
                res.push_back(str.substr(i,j-i));
                i=j;
            }
        }
		else if (i!= str.size() && str[i] == delimiter )
            i++;
		size_t j = i;
		while (j != str.size() && str[j] != delimiter )
			j++;
		if (i!=j){
			res.push_back(str.substr(i,j-i));
			i=j;
		}

	}
	return res;

}

void PhysicalCopy::GetChunk(DataChunk &result_chunk,
                            PhysicalOperatorState *state_) {
	result_chunk.Reset();
	if (state_->finished) {
		return;
	}
	DataChunk insert_chunk;
	auto types = table->GetTypes();
	insert_chunk.Initialize(types);

	int64_t count_line = 0;
	int64_t total = 0;

	if (is_from) {
		string value;
		std::ifstream from_csv;
		from_csv.open(file_path);
		while (getline(from_csv, value)) {
			if (count_line == insert_chunk.maximum_size) {
				insert_chunk.count = insert_chunk.data[0]->count;
				table->storage->AddData(insert_chunk);
				total += count_line;
				count_line = 0;
				insert_chunk.Reset();
			}
			std::vector<string> csv_line = split(value, delimiter,quote);

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
		size_t current_chunk = 0;
		size_t chunk_count = table->storage->columns[0]->data.size();
		while(current_chunk < chunk_count) {
			for (size_t col = 0; col < insert_chunk.column_count; col++) {
				auto column = table->storage->columns[col].get();
				insert_chunk.data[col]->Reference(*column->data[current_chunk]);
			}
			insert_chunk.count = insert_chunk.data[0]->count;
			for(size_t i = 0; i < insert_chunk.count; i++) {
				for (size_t col = 0; col < insert_chunk.column_count; col++) {
					if (col != 0) {
						to_csv << delimiter;
					}
                    if (types[col] == TypeId::VARCHAR)
                        to_csv << quote;
                    to_csv << insert_chunk.data[col]
                            ->GetValue(i)
                            .ToString();
                    if (types[col] == TypeId::VARCHAR)
                        to_csv << quote;
				}
                to_csv << endl;
                count_line++;
			}
			current_chunk++;
		}
		to_csv.close();
	}
	result_chunk.data[0]->count = 1;
	result_chunk.data[0]->SetValue(
	    0, Value::BIGINT(total + count_line));
	result_chunk.count = 1;
	state_->finished = true;
}

unique_ptr<PhysicalOperatorState>
PhysicalCopy::GetOperatorState(ExpressionExecutor *executor) {
	return make_unique<PhysicalOperatorState>(nullptr, executor);
}
