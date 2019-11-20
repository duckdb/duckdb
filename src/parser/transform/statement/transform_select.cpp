#include "duckdb/parser/statement/select_statement.hpp"
#include "duckdb/parser/transformer.hpp"
#include "duckdb/common/string_util.hpp"

using namespace duckdb;
using namespace std;

unique_ptr<SelectStatement> Transformer::TransformSelect(PGNode *node) {
	auto stmt = reinterpret_cast<PGSelectStmt *>(node);
	auto result = make_unique<SelectStatement>();

	if (stmt->windowClause) {
		for (auto window_ele = stmt->windowClause->head; window_ele != NULL; window_ele = window_ele->next) {
			auto window_def = reinterpret_cast<PGWindowDef *>(window_ele->data.ptr_value);
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
		TransformCTE(reinterpret_cast<PGWithClause *>(stmt->withClause), *result);
	}

	result->node = TransformSelectNode(stmt);
	return result;
}
