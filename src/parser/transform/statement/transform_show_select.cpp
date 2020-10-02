#include "duckdb/parser/statement/show_statement.hpp"
#include "duckdb/parser/sql_statement.hpp"
#include "duckdb/parser/transformer.hpp"
#include <iostream>

using namespace duckdb;
using namespace std;

unique_ptr<ShowStatement> Transformer::TransformShowSelect(PGNode *node) {
	// we transform SHOW x into PRAGMA SHOW('x')
	cout << "Here\n";
	auto stmt = reinterpret_cast<PGVariableShowStmtSelect *>(node);
	//auto select_stmt = reinterpret_cast<PGSelectStmt *>(stmt->stmt);
	cout << "Here222\n";
	if(string(stmt->name) == "select"){
		cout << "select statement\n";
		auto result = make_unique<ShowStatement>();
		result->selectStatement = TransformStatement(stmt->stmt);
		//result->selectStatement->node = TransformSelectNode(select_stmt);
		return result;
	}

}
