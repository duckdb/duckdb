#include <memory>
#include <utility>

#include "duckdb/parser/statement/delete_statement.hpp"
#include "duckdb/parser/query_node/delete_query_node.hpp"
#include "duckdb/parser/transformer.hpp"
#include "duckdb/common/enums/tableref_type.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/helper.hpp"
#include "duckdb/common/optional_ptr.hpp"
#include "duckdb/common/unique_ptr.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/parser/parsed_expression.hpp"
#include "duckdb/parser/tableref.hpp"
#include "nodes/nodes.hpp"
#include "nodes/parsenodes.hpp"
#include "nodes/pg_list.hpp"

namespace duckdb {

unique_ptr<DeleteStatement> Transformer::TransformDelete(duckdb_libpgquery::PGDeleteStmt &stmt) {
	auto result = make_uniq<DeleteStatement>();
	auto &node = *result->node;
	if (stmt.withClause) {
		TransformCTE(*PGPointerCast<duckdb_libpgquery::PGWithClause>(stmt.withClause), node.cte_map);
	}

	node.condition = TransformExpression(stmt.whereClause);
	node.table = TransformRangeVar(*stmt.relation);
	if (node.table->type != TableReferenceType::BASE_TABLE) {
		throw InvalidInputException("Can only delete from base tables!");
	}
	if (stmt.usingClause) {
		for (auto n = stmt.usingClause->head; n != nullptr; n = n->next) {
			auto target = PGPointerCast<duckdb_libpgquery::PGNode>(n->data.ptr_value);
			auto using_entry = TransformTableRefNode(*target);
			node.using_clauses.push_back(std::move(using_entry));
		}
	}

	if (stmt.returningList) {
		TransformExpressionList(*stmt.returningList, node.returning_list);
	}

	return result;
}

} // namespace duckdb
