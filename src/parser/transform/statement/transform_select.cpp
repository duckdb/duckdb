#include "duckdb/parser/statement/select_statement.hpp"
#include "duckdb/parser/transformer.hpp"
#include "duckdb/common/string_util.hpp"

namespace duckdb {

unique_ptr<QueryNode> Transformer::TransformSelectNode(duckdb_libpgquery::PGSelectStmt &select) {
	if (select.pivot) {
		return TransformPivotStatement(select);
	} else {
		return TransformSelectInternal(select);
	}
}

unique_ptr<SelectStatement> Transformer::TransformSelect(duckdb_libpgquery::PGSelectStmt &select, bool is_select) {
	auto result = make_uniq<SelectStatement>();

	// Both Insert/Create Table As uses this.
	if (is_select) {
		if (select.intoClause) {
			throw ParserException("SELECT INTO not supported!");
		}
		if (select.lockingClause) {
			throw ParserException("SELECT locking clause is not supported!");
		}
	}

	result->node = TransformSelectNode(select);
	return result;
}

unique_ptr<SelectStatement> Transformer::TransformSelect(optional_ptr<duckdb_libpgquery::PGNode> node, bool is_select) {
	return TransformSelect(PGCast<duckdb_libpgquery::PGSelectStmt>(*node), is_select);
}

} // namespace duckdb
