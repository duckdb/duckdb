#include "main/materialized_query_result.hpp"

using namespace duckdb;
using namespace std;

MaterializedQueryResult::MaterializedQueryResult() : QueryResult(QueryResultType::MATERIALIZED_RESULT) {
}

MaterializedQueryResult::MaterializedQueryResult(vector<TypeId> types, vector<string> names)
    : QueryResult(QueryResultType::MATERIALIZED_RESULT, types, names) {
}

MaterializedQueryResult::MaterializedQueryResult(string error)
    : QueryResult(QueryResultType::MATERIALIZED_RESULT, error) {
}

Value MaterializedQueryResult::GetValue(size_t column, size_t index) {
	auto &data = collection.GetChunk(index).data[column];
	auto offset_in_chunk = index % STANDARD_VECTOR_SIZE;
	return data.GetValue(offset_in_chunk);
}

void MaterializedQueryResult::Print() {
	if (success) {
		for (auto &name : names) {
			printf("%s\t", name.c_str());
		}
		printf(" [ %zu ]\n", collection.count);
		for (auto &type : types) {
			printf("%s\t", TypeIdToString(type).c_str());
		}
		printf("\n");
		for (size_t j = 0; j < collection.count; j++) {
			for (size_t i = 0; i < collection.column_count(); i++) {
				printf("%s\t", collection.GetValue(i, j).ToString().c_str());
			}
			printf("\n");
		}
		printf("\n");
	} else {
		fprintf(stderr, "Query Error: %s\n", error.c_str());
	}
}

unique_ptr<DataChunk> MaterializedQueryResult::Fetch() {
	if (!success) {
		return nullptr;
	}
	if (collection.chunks.size() == 0) {
		return make_unique<DataChunk>();
	}
	auto chunk = move(collection.chunks[0]);
	collection.chunks.erase(collection.chunks.begin() + 0);
	return chunk;
}
