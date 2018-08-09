
#include "parser/parser.hpp"
#include "parser/postgres/pg_list.h"
#include "parser/postgres/pg_query.h"
#include "parser/postgres/pg_trigger.h"

#include <stdio.h>

#include "parser/transform.hpp"

using namespace duckdb;
using namespace std;

Parser::Parser() {}

bool Parser::ParseQuery(const char *query) {
	// first we use the postgres parser to parse the query
	auto context = pg_query_parse_init();
	auto result = pg_query_parse(query);

	// check if it succeeded
	this->success = false;
	if (result.error) {
		this->message = string(result.error->message) + "[" +
		                to_string(result.error->lineno) + ":" +
		                to_string(result.error->cursorpos) + "]";
		goto wrapup;
	}

	fprintf(stderr, "%s\n", query);
	try {
		// if it succeeded, we transform the Postgres parse tree into a list of
		// SQLStatements
		if (!TransformList(result.tree)) {
			goto wrapup;
		}
		this->success = true;
	} catch (Exception ex) {
		this->message = ex.GetMessage();
	} catch (...) {
		this->message = "UNHANDLED EXCEPTION TYPE THROWN IN PARSER!";
	}
wrapup:
	pg_query_parse_finish(context);
	pg_query_free_parse_result(result);
	return this->success;
}

bool Parser::TransformList(List *tree) {
	for (auto entry = tree->head; entry != nullptr; entry = entry->next) {
		auto stmt = TransformNode((Node *)entry->data.ptr_value);
		if (!stmt) {
			statements.clear();
			return false;
		}
		statements.push_back(move(stmt));
	}
	return true;
}

unique_ptr<SQLStatement> Parser::TransformNode(Node *stmt) {
	switch (stmt->type) {
	case T_SelectStmt:
		return TransformSelect(stmt);
	case T_CreateStmt:
		return TransformCreate(stmt);
	case T_InsertStmt:
		return TransformInsert(stmt);
	case T_CopyStmt:
		return TransformCopy(stmt);
	default:
		throw NotImplementedException("A_Expr not implemented!");
	}
	return nullptr;
}
