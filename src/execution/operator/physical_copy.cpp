#include "execution/operator/physical_copy.hpp"
#include "storage/data_table.hpp"
#include <fstream>

using namespace duckdb;
using namespace std;

void PhysicalCopy::InitializeChunk(DataChunk &chunk) {
    vector<TypeId> types = {TypeId::INTEGER};
    chunk.Initialize(types);
}

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
    ifstream file (file_path);
    string value;
    insert_chunk.Initialize(types);
    int count_tuples = 0;
    while (getline ( file, value ))
    {
        count_tuples++;
        insert_chunk.data[0]->SetValue(0, Value(value));
    }

    insert_chunk.data[0]->count = count_tuples;
    insert_chunk.count = count_tuples;

    table->storage->AddData(insert_chunk);

    result_chunk.data[0]->SetValue(
            0, Value::NumericValue(TypeId::INTEGER, insert_chunk.data[0]->count));
    result_chunk.count = 1;
    state_->finished = true;

}

unique_ptr<PhysicalOperatorState> PhysicalCopy::GetOperatorState() {
    return make_unique<PhysicalOperatorState>(nullptr);
}
