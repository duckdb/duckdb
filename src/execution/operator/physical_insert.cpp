#include "execution/operator/physical_insert.hpp"
#include "execution/expression_executor.hpp"

#include "storage/data_table.hpp"

using namespace duckdb;
using namespace std;

void PhysicalInsert::InitializeChunk(DataChunk &chunk) {
	vector<TypeId> types = {TypeId::INTEGER};
	chunk.Initialize(types);
}

void PhysicalInsert::GetChunk(DataChunk &result_chunk,
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
	ExpressionExecutor executor(state_->child_chunk);

	for (size_t i = 0; i < value_list.size(); i++) {
		auto &expr = value_list[i];
		executor.Execute(expr.get(), *insert_chunk.data[i]);
	}
	insert_chunk.count = insert_chunk.data[0]->count;

	for (size_t i = 0; i < insert_chunk.column_count; i++) {
		if (insert_chunk.count != insert_chunk.data[i]->count) {
			throw Exception("Insert count mismatch!");
		}
	}
	result_chunk.data[0]->count = 1;
	result_chunk.data[0]->SetValue(
	    0, Value::NumericValue(TypeId::INTEGER, insert_chunk.data[0]->count));

	table->storage->AddData(insert_chunk);

	result_chunk.count = 1;
	state_->finished = true;
}

unique_ptr<PhysicalOperatorState> PhysicalInsert::GetOperatorState() {
	return make_unique<PhysicalOperatorState>(nullptr);
}
