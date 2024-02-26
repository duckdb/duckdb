#include "duckdb/parser/parser.hpp"
#include "duckdb/common/helper.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/function/table/range.hpp"
#include "duckdb/parser/tableref/subqueryref.hpp"

namespace duckdb {

static unique_ptr<TableRef> QueryBindReplace(ClientContext &context, TableFunctionBindInput &input) {
	string query = input.inputs[0].ToString();
	Parser parser(context.GetParserOptions());
	parser.ParseQuery(query);
	if (parser.statements.size() != 1 || parser.statements[0]->type != StatementType::SELECT_STATEMENT) {
		throw ParserException("Expected a single SELECT statement");
	}

	auto select_stmt = unique_ptr_cast<SQLStatement, SelectStatement>(std::move(parser.statements[0]));
	auto subquery_ref = duckdb::make_uniq<SubqueryRef>(std::move(select_stmt));
	return std::move(subquery_ref);
}

static unique_ptr<TableRef> TableBindReplace(ClientContext &context, TableFunctionBindInput &input) {
	D_ASSERT(input.named_parameters.size() == 0 || input.named_parameters.size() == 1);
	string by_name {""}; // by_name defaults to false
	if (input.named_parameters.size() == 1) {
		auto it = input.named_parameters.find("by_name");
		by_name = (it != input.named_parameters.end() && it->second.GetValue<bool>()) ? "BY NAME " : "";
	}
	// prepare the query
	string union_all_clause = " UNION ALL " + by_name + "FROM ";
	string query = "FROM " + input.inputs[0].ToString();
	for (size_t i = 1; i < input.inputs.size(); ++i) {
		query += union_all_clause + input.inputs[i].ToString();
	}

	Parser parser(context.GetParserOptions());
	parser.ParseQuery(query);
	if (parser.statements.size() != 1 || parser.statements[0]->type != StatementType::SELECT_STATEMENT) {
		throw ParserException("Expected a table as value");
	}
	auto select_stmt = unique_ptr_cast<SQLStatement, SelectStatement>(std::move(parser.statements[0]));
	auto subquery_ref = duckdb::make_uniq<SubqueryRef>(std::move(select_stmt));
	return std::move(subquery_ref);
}

void QueryTableFunction::RegisterFunction(BuiltinFunctions &set) {
	TableFunction query("query", {LogicalType::VARCHAR}, nullptr, nullptr);
	query.bind_replace = QueryBindReplace;
	set.AddFunction(query);

	TableFunction query_table("query_table", {LogicalType::VARCHAR}, nullptr, nullptr);
	query_table.named_parameters["by_name"] = LogicalType::BOOLEAN;
	query_table.bind_replace = TableBindReplace;
	query_table.varargs = LogicalType::VARCHAR;
	set.AddFunction(query_table);
}

} // namespace duckdb
