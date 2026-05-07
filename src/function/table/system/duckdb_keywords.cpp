#include "duckdb/function/table/system_functions.hpp"

#include "duckdb/common/exception.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/parser/parser.hpp"

namespace duckdb {

struct DuckDBKeywordsData : public GlobalTableFunctionState {
	DuckDBKeywordsData() : offset(0) {
	}

	vector<ParserKeyword> entries;
	idx_t offset;
};

static unique_ptr<FunctionData> DuckDBKeywordsBind(ClientContext &context, TableFunctionBindInput &input,
                                                   vector<LogicalType> &return_types, vector<string> &names) {
	names.emplace_back("keyword_name");
	return_types.emplace_back(LogicalType::VARCHAR);

	names.emplace_back("keyword_category");
	return_types.emplace_back(LogicalType::VARCHAR);

	return nullptr;
}

unique_ptr<GlobalTableFunctionState> DuckDBKeywordsInit(ClientContext &context, TableFunctionInitInput &input) {
	auto result = make_uniq<DuckDBKeywordsData>();
	result->entries = Parser::KeywordList();
	return std::move(result);
}

void DuckDBKeywordsFunction(ClientContext &context, TableFunctionInput &data_p, DataChunk &output) {
	auto &data = data_p.global_state->Cast<DuckDBKeywordsData>();
	if (data.offset >= data.entries.size()) {
		// finished returning values
		return;
	}
	// start returning values
	// either fill up the chunk or return all the remaining columns
	idx_t count = 0;

	// keyword_name, VARCHAR
	auto &keyword_name = output.data[0];
	// keyword_category, VARCHAR
	auto &keyword_category = output.data[1];

	while (data.offset < data.entries.size() && count < STANDARD_VECTOR_SIZE) {
		auto &entry = data.entries[data.offset++];

		keyword_name.Append(Value(entry.name));
		string category_name;
		switch (entry.category) {
		case KeywordCategory::KEYWORD_RESERVED:
			category_name = "reserved";
			break;
		case KeywordCategory::KEYWORD_UNRESERVED:
			category_name = "unreserved";
			break;
		case KeywordCategory::KEYWORD_TYPE_FUNC:
			category_name = "type_function";
			break;
		case KeywordCategory::KEYWORD_COL_NAME:
			category_name = "column_name";
			break;
		default:
			throw InternalException("Unrecognized keyword category");
		}
		keyword_category.Append(Value(std::move(category_name)));

		count++;
	}
	output.SetCardinality(count);
}

void DuckDBKeywordsFun::RegisterFunction(BuiltinFunctions &set) {
	set.AddFunction(
	    TableFunction("duckdb_keywords", {}, DuckDBKeywordsFunction, DuckDBKeywordsBind, DuckDBKeywordsInit));
}

} // namespace duckdb
