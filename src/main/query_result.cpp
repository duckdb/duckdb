#include "duckdb/main/query_result.hpp"
#include "duckdb/common/printer.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/common/box_renderer.hpp"
namespace duckdb {

BaseQueryResult::BaseQueryResult(QueryResultType type, StatementType statement_type, StatementProperties properties_p,
                                 vector<LogicalType> types_p, vector<string> names_p)
    : type(type), statement_type(statement_type), properties(std::move(properties_p)), types(std::move(types_p)),
      names(std::move(names_p)), success(true) {
	D_ASSERT(types.size() == names.size());
}

BaseQueryResult::BaseQueryResult(QueryResultType type, PreservedError error)
    : type(type), success(false), error(std::move(error)) {
}

BaseQueryResult::~BaseQueryResult() {
}

void BaseQueryResult::ThrowError(const string &prepended_message) const {
	D_ASSERT(HasError());
	error.Throw(prepended_message);
}

void BaseQueryResult::SetError(PreservedError error) {
	success = !error;
	this->error = std::move(error);
}

bool BaseQueryResult::HasError() const {
	D_ASSERT((bool)error == !success);
	return !success;
}

const ExceptionType &BaseQueryResult::GetErrorType() const {
	return error.Type();
}

const std::string &BaseQueryResult::GetError() {
	D_ASSERT(HasError());
	return error.Message();
}

PreservedError &BaseQueryResult::GetErrorObject() {
	return error;
}

idx_t BaseQueryResult::ColumnCount() {
	return types.size();
}

QueryResult::QueryResult(QueryResultType type, StatementType statement_type, StatementProperties properties,
                         vector<LogicalType> types_p, vector<string> names_p, ClientProperties client_properties_p)
    : BaseQueryResult(type, statement_type, std::move(properties), std::move(types_p), std::move(names_p)),
      client_properties(std::move(client_properties_p)) {
}

QueryResult::QueryResult(QueryResultType type, PreservedError error)
    : BaseQueryResult(type, std::move(error)), client_properties("UTC", ArrowOffsetSize::REGULAR) {
}

QueryResult::~QueryResult() {
}

const string &QueryResult::ColumnName(idx_t index) const {
	D_ASSERT(index < names.size());
	return names[index];
}

string QueryResult::ToBox(ClientContext &context, const BoxRendererConfig &config) {
	return ToString();
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

} // namespace duckdb
