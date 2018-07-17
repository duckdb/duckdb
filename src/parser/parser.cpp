
#include "parser/parser.hpp"
#include "parser/pg_list.h"
#include "parser/pg_query.h"
#include "parser/pg_trigger.h"

#include <stdio.h>

#include "parser/transform.hpp"

using namespace duckdb;
using namespace std;

bool ParseList(Parser &parser, List *tree);
SQLStatement *ParseNode(Node *node);

Parser::Parser() {}

bool Parser::ParseQuery(const char *query) {
	auto context = pg_query_parse_init();
	auto result = pg_query_parse(query);

	this->success = false;
	if (result.error) {
		this->message = string(result.error->message) + "[" +
		                to_string(result.error->lineno) + ":" +
		                to_string(result.error->cursorpos) + "]";
		goto wrapup;
	}
	print_pg_parse_tree(result.tree);
	try {
		if (!ParseList(result.tree)) {
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

bool Parser::ParseList(List *tree) {
	for (auto entry = tree->head; entry != nullptr; entry = entry->next) {
		auto stmt = ParseNode((Node *)entry->data.ptr_value);
		if (!stmt) {
			statements.clear();
			return false;
		}
		statements.push_back(move(stmt));
	}
	return true;
}

std::unique_ptr<SQLStatement> Parser::ParseNode(Node *stmt) {
	switch (stmt->type) {
	case T_SelectStmt:
		return TransformSelect(stmt);
	default:
		throw NotImplementedException("A_Expr not implemented!");
	}
	return nullptr;
}
