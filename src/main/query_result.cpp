#include "duckdb/main/query_result.hpp"

#include "duckdb/common/box_renderer.hpp"
#include "duckdb/common/printer.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/main/client_context.hpp"
namespace duckdb {

BaseQueryResult::BaseQueryResult(QueryResultType type, StatementType statement_type, StatementProperties properties_p,
                                 vector<LogicalType> types_p, vector<string> names_p)
    : type(type), statement_type(statement_type), properties(std::move(properties_p)), types(std::move(types_p)),
      names(std::move(names_p)), success(true) {
	D_ASSERT(types.size() == names.size());
}

BaseQueryResult::BaseQueryResult(QueryResultType type, ErrorData error)
    : type(type), success(false), error(std::move(error)) {
	// Assert that the error object is initialized
	D_ASSERT(this->error.HasError());
}

BaseQueryResult::~BaseQueryResult() {
}

void BaseQueryResult::ThrowError(const string &prepended_message) const {
	D_ASSERT(HasError());
	error.Throw(prepended_message);
}

void BaseQueryResult::SetError(ErrorData error) {
	success = !error.HasError();
	this->error = std::move(error);
}

bool BaseQueryResult::HasError() const {
	D_ASSERT(error.HasError() == !success);
	return !success;
}

const ExceptionType &BaseQueryResult::GetErrorType() const {
	return error.Type();
}

const std::string &BaseQueryResult::GetError() {
	D_ASSERT(HasError());
	return error.Message();
}

ErrorData &BaseQueryResult::GetErrorObject() {
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

QueryResult::QueryResult(QueryResultType type, ErrorData error)
    : BaseQueryResult(type, std::move(error)), client_properties("UTC", ArrowOffsetSize::REGULAR, false, false) {
}

QueryResult::~QueryResult() {
}

void QueryResult::DeduplicateColumns(vector<string> &names) {
	unordered_map<string, idx_t> name_map;
	for (auto &column_name : names) {
		// put it all lower_case
		auto low_column_name = StringUtil::Lower(column_name);
		if (name_map.find(low_column_name) == name_map.end()) {
			// Name does not exist yet
			name_map[low_column_name]++;
		} else {
			// Name already exists, we add _x where x is the repetition number
			string new_column_name = column_name + "_" + std::to_string(name_map[low_column_name]);
			auto new_column_name_low = StringUtil::Lower(new_column_name);
			while (name_map.find(new_column_name_low) != name_map.end()) {
				// This name is already here due to a previous definition
				name_map[low_column_name]++;
				new_column_name = column_name + "_" + std::to_string(name_map[low_column_name]);
				new_column_name_low = StringUtil::Lower(new_column_name);
			}
			column_name = new_column_name;
			name_map[new_column_name_low]++;
		}
	}
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
	unique_ptr<DataChunk> lchunk, rchunk;
	idx_t lindex = 0, rindex = 0;
	while (true) {
		if (!lchunk || lindex == lchunk->size()) {
			lchunk = Fetch();
			lindex = 0;
		}
		if (!rchunk || rindex == rchunk->size()) {
			rchunk = other.Fetch();
			rindex = 0;
		}
		if (!lchunk && !rchunk) {
			return true;
		}
		if (!lchunk || !rchunk) {
			return false;
		}
		if (lchunk->size() == 0 && rchunk->size() == 0) {
			return true;
		}
		D_ASSERT(lchunk->ColumnCount() == rchunk->ColumnCount());
		for (; lindex < lchunk->size() && rindex < rchunk->size(); lindex++, rindex++) {
			for (idx_t col = 0; col < rchunk->ColumnCount(); col++) {
				auto lvalue = lchunk->GetValue(col, lindex);
				auto rvalue = rchunk->GetValue(col, rindex);
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
