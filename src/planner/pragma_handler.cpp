#include "duckdb/planner/pragma_handler.hpp"

#include "duckdb/parser/statement/select_statement.hpp"
#include "duckdb/parser/query_node/select_node.hpp"
#include "duckdb/parser/expression/constant_expression.hpp"
#include "duckdb/parser/expression/star_expression.hpp"
#include "duckdb/parser/tableref/table_function_ref.hpp"
#include "duckdb/parser/expression/columnref_expression.hpp"
#include "duckdb/parser/expression/function_expression.hpp"
#include "duckdb/parser/parsed_data/pragma_info.hpp"
#include "duckdb/parser/parser.hpp"

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
		Parser parser;
		parser.ParseQuery("SELECT * FROM pragma_table_info()");

		// push the table name parameter into the table function
		auto select_statement = move(parser.statements[0]);
		auto &select = (SelectStatement &)*select_statement;
		auto &select_node = (SelectNode &)*select.node;
		auto &table_function = (TableFunctionRef &)*select_node.from_table;
		auto &function = (FunctionExpression &)*table_function.function;
		function.children.push_back(make_unique<ConstantExpression>(SQLTypeId::VARCHAR, pragma.parameters[0]));
		return select_statement;
	} else if (keyword == "show_tables") {
		if (pragma.pragma_type != PragmaType::NOTHING) {
			throw ParserException("Invalid PRAGMA show_tables: cannot be called");
		}
		// turn into SELECT name FROM sqlite_master();
		Parser parser;
		parser.ParseQuery("SELECT name FROM sqlite_master() ORDER BY name");
		return move(parser.statements[0]);
	} else if (keyword == "collations") {
		if (pragma.pragma_type != PragmaType::NOTHING) {
			throw ParserException("Invalid PRAGMA collations: cannot be called");
		}
		// turn into SELECT * FROM pragma_collations();
		Parser parser;
		parser.ParseQuery("SELECT * FROM pragma_collations() ORDER BY 1");
		return move(parser.statements[0]);
	} else if (keyword == "show") {
		if (pragma.pragma_type != PragmaType::CALL) {
			throw ParserException("Invalid PRAGMA show_tables: expected a function call");
		}
		if (pragma.parameters.size() != 1) {
			throw ParserException("Invalid PRAGMA show_tables: show_tables does not take any arguments");
		}
		// PRAGMA table_info but with some aliases
		Parser parser;
		parser.ParseQuery(
		    "SELECT name AS \"Field\", type as \"Type\", CASE WHEN \"notnull\" THEN 'NO' ELSE 'YES' END AS \"Null\", "
		    "NULL AS \"Key\", dflt_value AS \"Default\", NULL AS \"Extra\" FROM pragma_table_info()");

		// push the table name parameter into the table function
		auto select_statement = move(parser.statements[0]);
		auto &select = (SelectStatement &)*select_statement;
		auto &select_node = (SelectNode &)*select.node;
		auto &table_function = (TableFunctionRef &)*select_node.from_table;
		auto &function = (FunctionExpression &)*table_function.function;
		function.children.push_back(make_unique<ConstantExpression>(SQLTypeId::VARCHAR, pragma.parameters[0]));
		return select_statement;
	}
	return nullptr;
}
