#include "parser/expression/star_expression.hpp"
#include "parser/statement/create_view_statement.hpp"
#include "parser/tableref/subqueryref.hpp"
#include "parser/transformer.hpp"

using namespace duckdb;
using namespace postgres;
using namespace std;

unique_ptr<CreateViewStatement> Transformer::TransformCreateView(Node *node) {
	assert(node);
	assert(node->type == T_ViewStmt);

	auto stmt = reinterpret_cast<ViewStmt *>(node);
	assert(stmt);
	assert(stmt->view);

	auto result = make_unique<CreateViewStatement>();
	auto &info = *result->info.get();

	if (stmt->view->schemaname) {
		info.schema = stmt->view->schemaname;
	}
	info.view_name = stmt->view->relname;
	info.replace = stmt->replace;

	info.query = TransformSelectNode((SelectStmt *)stmt->query);

	if (stmt->aliases && stmt->aliases->length > 0) {
		for (auto c = stmt->aliases->head; c != NULL; c = lnext(c)) {
			auto node = reinterpret_cast<Node *>(c->data.ptr_value);
			switch (node->type) {
			case T_String: {
				auto val = (postgres::Value *)node;
				info.aliases.push_back(string(val->val.str));
				break;
			}
			default:
				throw NotImplementedException("View projection type");
			}
		}
		if (info.aliases.size() < 1) {
			throw ParserException("Need at least one column name in CREATE VIEW projection list");
		}
	}

	if (stmt->options && stmt->options->length > 0) {
		throw NotImplementedException("VIEW options");
	}

	if (stmt->withCheckOption != ViewCheckOption::NO_CHECK_OPTION) {
		throw NotImplementedException("VIEW CHECK options");
	}

	return result;
}
