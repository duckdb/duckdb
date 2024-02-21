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
	string query = "FROM " + input.inputs[0].ToString();
	for (size_t i = 1; i < input.inputs.size(); ++i) {
		query += " UNION ALL FROM " + input.inputs[i].ToString();
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

	TableFunction table("query_table", {LogicalType::VARCHAR}, nullptr, nullptr);
	table.bind_replace = TableBindReplace;
	table.varargs = LogicalType::VARCHAR;
	set.AddFunction(table);
}

} // namespace duckdb
