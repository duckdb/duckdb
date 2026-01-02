#include "duckdb/parser/statement/select_statement.hpp"
#include "duckdb/parser/transformer.hpp"
#include "duckdb/common/string_util.hpp"

namespace duckdb {

unique_ptr<QueryNode> Transformer::TransformSelectNode(duckdb_libpgquery::PGNode &node, bool is_select) {
	switch (node.type) {
	case duckdb_libpgquery::T_PGVariableShowSelectStmt:
		return TransformShowSelect(PGCast<duckdb_libpgquery::PGVariableShowSelectStmt>(node));
	case duckdb_libpgquery::T_PGVariableShowStmt:
		return TransformShow(PGCast<duckdb_libpgquery::PGVariableShowStmt>(node));
	default:
		return TransformSelectNodeInternal(PGCast<duckdb_libpgquery::PGSelectStmt>(node), is_select);
	}
}

unique_ptr<QueryNode> Transformer::TransformSelectNodeInternal(duckdb_libpgquery::PGSelectStmt &select,
                                                               bool is_select) {
	// Both Insert/Create Table As uses this.
	if (is_select) {
		if (select.intoClause) {
			throw ParserException("SELECT INTO not supported!");
		}
		if (select.lockingClause) {
			throw ParserException("SELECT locking clause is not supported!");
		}
	}
	if (select.pivot) {
		return TransformPivotStatement(select);
	}
	return TransformSelectInternal(select);
}

unique_ptr<SelectStatement> Transformer::TransformSelectStmt(duckdb_libpgquery::PGSelectStmt &select, bool is_select) {
	auto result = make_uniq<SelectStatement>();
	result->node = TransformSelectNodeInternal(select, is_select);
	return result;
}

unique_ptr<SelectStatement> Transformer::TransformSelectStmt(duckdb_libpgquery::PGNode &node, bool is_select) {
	auto select_node = TransformSelectNode(node, is_select);
	auto select_statement = make_uniq<SelectStatement>();
	select_statement->node = std::move(select_node);
	return select_statement;
}

} // namespace duckdb
