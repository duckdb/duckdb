#include "duckdb/parser/statement/pragma_statement.hpp"
#include "duckdb/parser/statement/show_statement.hpp"
#include "duckdb/parser/transformer.hpp"
#include <iostream>

namespace duckdb {

using namespace duckdb_libpgquery;

unique_ptr<SQLStatement> Transformer::TransformShow(PGNode *node) {
	// we transform SHOW x into PRAGMA SHOW('x')
	cout << "Here\n";
	auto stmt = reinterpret_cast<PGVariableShowStmtSelect *>(node);
	auto select_stmt = reinterpret_cast<PGSelectStmt *>(stmt->stmt);
	cout << "Here222\n";
	if(string(stmt->name) == "select"){
		cout << "select statement\n";
		auto result = make_unique<ShowStatement>();
		result->selectStatement = make_unique<SelectStatement>();
		result->selectStatement->node = TransformSelectNode(select_stmt);
		return result;
	} /*else {

		auto result = make_unique<PragmaStatement>();
		auto &info = *result->info;

	if (string(stmt->name) == "tables") {
		// show all tables
		info.name = "show_tables";
	} else {
		// show one specific table
		info.name = "show";
		info.parameters.push_back(Value(stmt->name));
	}

}

} // namespace duckdb
