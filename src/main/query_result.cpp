#include "duckdb/main/query_result.hpp"
#include "duckdb/common/printer.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/main/client_context.hpp"

namespace duckdb {

BaseQueryResult::BaseQueryResult(QueryResultType type, StatementType statement_type, StatementProperties properties,
                                 vector<LogicalType> types_p, vector<string> names_p)
    : type(type), statement_type(statement_type), properties(properties), types(move(types_p)), names(move(names_p)),
      success(true) {
	D_ASSERT(types.size() == names.size());
}

BaseQueryResult::BaseQueryResult(QueryResultType type, string error) : type(type), success(false), error(move(error)) {
}

BaseQueryResult::~BaseQueryResult() {
}

bool BaseQueryResult::HasError() {
	return !success;
}
const string &BaseQueryResult::GetError() {
	return error;
}
idx_t BaseQueryResult::ColumnCount() {
	return types.size();
}

QueryResult::QueryResult(QueryResultType type, StatementType statement_type, StatementProperties properties,
                         vector<LogicalType> types_p, vector<string> names_p, ClientProperties client_properties_p)
    : BaseQueryResult(type, statement_type, properties, move(types_p), move(names_p)),
      client_properties(move(client_properties_p)) {
}

QueryResult::QueryResult(QueryResultType type, string error) : BaseQueryResult(type, move(error)) {
}

QueryResult::~QueryResult() {
}

unique_ptr<DataChunk> QueryResult::Fetch() {
	auto chunk = FetchRaw();
	if (!chunk) {
		return nullptr;
	}
	chunk->Flatten();
	return chunk;
}

bool QueryResult::Equals(QueryResult &other) { // LCOV_EXCL_START
	// first compare the success state of the results
	if (success != other.success) {
		return false;
	}
	if (!success) {
		return error == other.error;
	}
	// compare names
	if (names != other.names) {
		return false;
	}
	// compare types
	if (types != other.types) {
		return false;
	}
	// now compare the actual values
	// fetch chunks
	while (true) {
		auto lchunk = Fetch();
		auto rchunk = other.Fetch();
		if (!lchunk && !rchunk) {
			return true;
		}
		if (!lchunk || !rchunk) {
			return false;
		}
		if (lchunk->size() == 0 && rchunk->size() == 0) {
			return true;
		}
		if (lchunk->size() != rchunk->size()) {
			return false;
		}
		D_ASSERT(lchunk->ColumnCount() == rchunk->ColumnCount());
		for (idx_t col = 0; col < rchunk->ColumnCount(); col++) {
			for (idx_t row = 0; row < rchunk->size(); row++) {
				auto lvalue = lchunk->GetValue(col, row);
				auto rvalue = rchunk->GetValue(col, row);
				if (lvalue.IsNull() && rvalue.IsNull()) {
					continue;
				}
				if (lvalue.IsNull() != rvalue.IsNull()) {
					return false;
				}
				if (lvalue != rvalue) {
					return false;
				}
			}
		}
	}
} // LCOV_EXCL_STOP

void QueryResult::Print() {
	Printer::Print(ToString());
}

string QueryResult::HeaderToString() {
	string result;
	for (auto &name : names) {
		result += name + "\t";
	}
	result += "\n";
	for (auto &type : types) {
		result += type.ToString() + "\t";
	}
	result += "\n";
	return result;
}
string QueryResult::GetConfigTimezone(QueryResult &query_result) {
	return query_result.client_properties.timezone;
}

} // namespace duckdb
