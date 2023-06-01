#include "duckdb/parser/transformer.hpp"
#include "duckdb/parser/statement/detach_statement.hpp"
#include "duckdb/parser/expression/constant_expression.hpp"
#include "duckdb/common/string_util.hpp"

namespace duckdb {

unique_ptr<DetachStatement> Transformer::TransformDetach(duckdb_libpgquery::PGNode *node) {
	auto stmt = reinterpret_cast<duckdb_libpgquery::PGDetachStmt *>(node);
	auto result = make_uniq<DetachStatement>();
	auto info = make_uniq<DetachInfo>();
	info->name = stmt->db_name;
	info->if_exists = stmt->missing_ok;

	result->info = std::move(info);
	return result;
}

} // namespace duckdb
