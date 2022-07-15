#include "duckdb/main/materialized_query_result.hpp"
#include "duckdb/common/to_string.hpp"

namespace duckdb {

MaterializedQueryResult::MaterializedQueryResult(StatementType statement_type, StatementProperties properties,
                                                 vector<LogicalType> types, vector<string> names,
                                                 const shared_ptr<ClientContext> &context_p)
    : QueryResult(QueryResultType::MATERIALIZED_RESULT, statement_type, properties, move(types), move(names)),
      collection(Allocator::DefaultAllocator()), context(context_p) {
}

MaterializedQueryResult::MaterializedQueryResult(string error)
    : QueryResult(QueryResultType::MATERIALIZED_RESULT, move(error)), collection(Allocator::DefaultAllocator()) {
}

Value MaterializedQueryResult::GetValue(idx_t column, idx_t index) {
	auto &data = collection.GetChunkForRow(index).data[column];
	auto offset_in_chunk = index % STANDARD_VECTOR_SIZE;
	return data.GetValue(offset_in_chunk);
}

string MaterializedQueryResult::ToString() {
	string result;
	if (success) {
		result = HeaderToString();
		result += "[ Rows: " + to_string(collection.Count()) + "]\n";
		for (idx_t j = 0; j < collection.Count(); j++) {
			for (idx_t i = 0; i < collection.ColumnCount(); i++) {
				auto val = collection.GetValue(i, j);
				result += val.IsNull() ? "NULL" : val.ToString();
				result += "\t";
			}
			result += "\n";
		}
		result += "\n";
	} else {
		result = error + "\n";
	}
	return result;
}

unique_ptr<DataChunk> MaterializedQueryResult::Fetch() {
	return FetchRaw();
}

unique_ptr<DataChunk> MaterializedQueryResult::FetchRaw() {
	if (!success) {
		throw InvalidInputException("Attempting to fetch from an unsuccessful query result\nError: %s", error);
	}
	return collection.Fetch();
}

} // namespace duckdb
