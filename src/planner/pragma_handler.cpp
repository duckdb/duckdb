#include "duckdb/planner/pragma_handler.hpp"

#include "duckdb/parser/statement/select_statement.hpp"
#include "duckdb/parser/query_node/select_node.hpp"
#include "duckdb/parser/expression/constant_expression.hpp"
#include "duckdb/parser/expression/star_expression.hpp"
#include "duckdb/parser/tableref/table_function_ref.hpp"
#include "duckdb/parser/expression/function_expression.hpp"

#include "duckdb/parser/parsed_data/pragma_info.hpp"

#include "duckdb/common/string_util.hpp"

using namespace duckdb;
using namespace std;

PragmaHandler::PragmaHandler(ClientContext &context) : context(context) {
}

unique_ptr<SQLStatement> PragmaHandler::HandlePragma(PragmaInfo &pragma) {
	string keyword = StringUtil::Lower(pragma.name);
	if (keyword == "table_info") {
		if (pragma.pragma_type != PragmaType::CALL) {
			throw ParserException("Invalid PRAGMA table_info: expected table name");
		}
		if (pragma.parameters.size() != 1) {
			throw ParserException("Invalid PRAGMA table_info: table_info takes exactly one argument");
		}
		// generate a SelectStatement that selects from the pragma_table_info function
		// i.e. SELECT * FROM pragma_table_info('table_name')
		auto select_statement = make_unique<SelectStatement>();
		auto select_node = make_unique<SelectNode>();
		select_node->select_list.push_back(make_unique<StarExpression>());

		vector<unique_ptr<ParsedExpression>> children;
		children.push_back(make_unique<ConstantExpression>(SQLTypeId::VARCHAR, pragma.parameters[0]));
		auto table_function = make_unique<TableFunctionRef>();
		table_function->function = make_unique<FunctionExpression>(DEFAULT_SCHEMA, "pragma_table_info", children);
		select_node->from_table = move(table_function);
		select_statement->node = move(select_node);
		return move(select_statement);
	}
	return nullptr;
}
