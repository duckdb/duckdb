#include "parser/statement/select_statement.hpp"
#include "parser/transformer.hpp"
#include "common/string_util.hpp"

using namespace duckdb;
using namespace postgres;
using namespace std;

unique_ptr<SelectStatement> Transformer::TransformSelect(Node *node) {
	SelectStmt *stmt = reinterpret_cast<SelectStmt *>(node);
	auto result = make_unique<SelectStatement>();

	if (stmt->windowClause) {
		for (auto window_ele = stmt->windowClause->head; window_ele != NULL; window_ele = window_ele->next) {
			auto window_def = reinterpret_cast<WindowDef *>(window_ele->data.ptr_value);
			assert(window_def);
			assert(window_def->name);
			auto window_name = StringUtil::Lower(string(window_def->name));

			auto it = window_clauses.find(window_name);
			if (it != window_clauses.end()) {
				throw Exception("A window specification needs an unique name");
			}
			window_clauses[window_name] = window_def;
		}
	}

	// may contain windows so second
	if (stmt->withClause) {
		TransformCTE(reinterpret_cast<WithClause *>(stmt->withClause), *result);
	}

	result->node = TransformSelectNode(stmt);
	return result;
}
