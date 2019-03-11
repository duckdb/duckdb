#include "main/stream_query_result.hpp"

#include "main/client_context.hpp"
#include "main/materialized_query_result.hpp"

using namespace duckdb;
using namespace std;

StreamQueryResult::StreamQueryResult(ClientContext &context, vector<TypeId> types, vector<string> names)
    : QueryResult(QueryResultType::STREAM_RESULT, types, names), is_open(true), context(context) {
}

StreamQueryResult::~StreamQueryResult() {
	Close();
}

void StreamQueryResult::Print() {
	if (success) {
		for (auto &name : names) {
			printf("%s\t", name.c_str());
		}
		printf("\n");
		for (auto &type : types) {
			printf("%s\t", TypeIdToString(type).c_str());
		}
		printf("\n");
		printf("[[STREAM RESULT]]\n");
	} else {
		fprintf(stderr, "Query Error: %s\n", error.c_str());
	}
}

unique_ptr<DataChunk> StreamQueryResult::Fetch() {
	if (!success || !is_open) {
		return nullptr;
	}
	auto chunk = context.Fetch();
	if (!chunk || chunk->column_count == 0 || chunk->size() == 0) {
		Close();
	}
	return chunk;
}

unique_ptr<MaterializedQueryResult> StreamQueryResult::Materialize() {
	if (!success) {
		return make_unique<MaterializedQueryResult>(error);
	}
	auto result = make_unique<MaterializedQueryResult>(types, names);
	while (true) {
		auto chunk = Fetch();
		if (!chunk || chunk->size() == 0) {
			return result;
		}
		result->collection.Append(*chunk);
	}
}

void StreamQueryResult::Close() {
	if (!is_open) {
		return;
	}
	context.Cleanup();
}
