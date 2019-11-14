#include "duckdb/parser/expression/star_expression.hpp"
#include "duckdb/parser/statement/create_view_statement.hpp"
#include "duckdb/parser/tableref/subqueryref.hpp"
#include "duckdb/parser/transformer.hpp"

using namespace duckdb;
using namespace std;

unique_ptr<CreateViewStatement> Transformer::TransformCreateView(postgres::Node *node) {
	assert(node);
	assert(node->type == postgres::T_ViewStmt);

	auto stmt = reinterpret_cast<postgres::ViewStmt *>(node);
	assert(stmt);
	assert(stmt->view);

	auto result = make_unique<CreateViewStatement>();
	auto &info = *result->info.get();

	if (stmt->view->schemaname) {
		info.schema = stmt->view->schemaname;
	}
	info.view_name = stmt->view->relname;
	info.replace = stmt->replace;

	info.query = TransformSelectNode((postgres::SelectStmt *)stmt->query);

	if (stmt->aliases && stmt->aliases->length > 0) {
		for (auto c = stmt->aliases->head; c != NULL; c = lnext(c)) {
			auto node = reinterpret_cast<postgres::Node *>(c->data.ptr_value);
			switch (node->type) {
			case postgres::T_String: {
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

	if (stmt->withCheckOption != postgres::ViewCheckOption::NO_CHECK_OPTION) {
		throw NotImplementedException("VIEW CHECK options");
	}

	return result;
}
