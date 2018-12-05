#include "parser/statement/select_statement.hpp"
#include "parser/transformer.hpp"

using namespace duckdb;
using namespace postgres;
using namespace std;

unique_ptr<SelectStatement> Transformer::TransformSelect(Node *node) {
	SelectStmt *stmt = reinterpret_cast<SelectStmt *>(node);
	auto result = make_unique<SelectStatement>();

	if (stmt->withClause) {
		TransformCTE(reinterpret_cast<WithClause *>(stmt->withClause), *result);
	}

	result->node = TransformSelectNode(stmt);
	return result;
}
