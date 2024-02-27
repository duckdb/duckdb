#include "duckdb/parser/parser.hpp"
#include "duckdb/common/helper.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/function/table/range.hpp"
#include "duckdb/function/function_set.hpp"
#include "duckdb/parser/tableref/subqueryref.hpp"

namespace duckdb {

static unique_ptr<SubqueryRef> parse_subquery(const string &query, const ParserOptions &options,
                                              const string &error_msg) {
	Parser parser(options);
	parser.ParseQuery(query);
	if (parser.statements.size() != 1 || parser.statements[0]->type != StatementType::SELECT_STATEMENT) {
		throw ParserException(error_msg);
	}
	auto select_stmt = unique_ptr_cast<SQLStatement, SelectStatement>(std::move(parser.statements[0]));
	return duckdb::make_uniq<SubqueryRef>(std::move(select_stmt));
}

static void union_tables_query(TableFunctionBindInput &input, string &query) {
	auto it = input.named_parameters.find("by_name");
	string by_name = (it != input.named_parameters.end() && it->second.GetValue<bool>())
	                     ? "BY NAME "
	                     : ""; // 'by_name' variable defaults to false
	if (input.inputs[0].type().id() == LogicalTypeId::VARCHAR) {
		query += "FROM " + input.inputs[0].ToString();
	} else if (input.inputs[0].type() == LogicalType::LIST(LogicalType::VARCHAR)) {
		string union_all_clause = " UNION ALL " + by_name + "FROM ";
		const auto &children = ListValue::GetChildren(input.inputs[0]);
		if (children.empty()) {
			throw InvalidInputException("Input list is empty");
		}
		query += "FROM " + children[0].ToString();
		for (size_t i = 1; i < children.size(); ++i) {
			auto child = children[i].ToString();
			query += union_all_clause + child;
		}
	}
}

static unique_ptr<TableRef> QueryBindReplace(ClientContext &context, TableFunctionBindInput &input) {
	auto query = input.inputs[0].ToString();
	auto subquery_ref = parse_subquery(query, context.GetParserOptions(), "Expected a single SELECT statement");
	return subquery_ref;
}

static unique_ptr<TableRef> TableBindReplace(ClientContext &context, TableFunctionBindInput &input) {
	D_ASSERT(input.named_parameters.size() == 0 || input.named_parameters.size() == 1); // expected only by_name arg
	string err_msg = "Expected a table or a list with tables as input";
	if (input.inputs.size() != 1) {
		throw ParserException(err_msg);
	}
	string query;
	union_tables_query(input, query);
	auto subquery_ref = parse_subquery(query, context.GetParserOptions(), err_msg);
	return subquery_ref;
}

void QueryTableFunction::RegisterFunction(BuiltinFunctions &set) {
	TableFunction query("query", {LogicalType::VARCHAR}, nullptr, nullptr);
	query.bind_replace = QueryBindReplace;
	set.AddFunction(query);

	TableFunctionSet query_table("query_table");
	TableFunction query_table_function({LogicalType::LIST(LogicalType::VARCHAR)}, nullptr, nullptr);
	query_table_function.named_parameters["by_name"] = LogicalType::BOOLEAN;
	query_table_function.bind_replace = TableBindReplace;
	query_table.AddFunction(query_table_function);

	query_table_function.arguments = {LogicalType::VARCHAR};
	query_table.AddFunction(query_table_function);
	set.AddFunction(query_table);
}

} // namespace duckdb
