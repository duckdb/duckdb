
#include "expression_helper.hpp"
#include "parser/parser.hpp"

#include "parser/statement/select_statement.hpp"

using namespace std;

namespace duckdb {

unique_ptr<Expression> ParseExpression(std::string expression) {
	string query = "SELECT " + expression;

	try {
		Parser parser;
		if (!parser.ParseQuery(query.c_str())) {
			return nullptr;
		}
		if (parser.statements.size() == 0 ||
		    parser.statements[0]->type != StatementType::SELECT) {
			return nullptr;
		}
		auto &select = *((SelectStatement *)parser.statements[0].get());
		return move(select.select_list[0]);
	} catch (...) {
		return nullptr;
	}
}

} // namespace duckdb
