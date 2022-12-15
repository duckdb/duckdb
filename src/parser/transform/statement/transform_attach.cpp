#include "duckdb/parser/transformer.hpp"
#include "duckdb/parser/statement/attach_statement.hpp"

namespace duckdb {

unique_ptr<AttachStatement> Transformer::TransformAttach(duckdb_libpgquery::PGNode *node) {
	auto stmt = reinterpret_cast<duckdb_libpgquery::PGAttachStmt *>(node);
	auto result = make_unique<AttachStatement>();
	auto info = make_unique<AttachInfo>();
	info->name = stmt->name ? stmt->name : string();
	info->path = stmt->path;
	result->info = move(info);
	return result;
}

} // namespace duckdb
