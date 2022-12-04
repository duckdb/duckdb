#include "duckdb/parser/statement/pragma_statement.hpp"
#include "duckdb/parser/transformer.hpp"

namespace duckdb {

unique_ptr<PragmaStatement> Transformer::TransformAttach(duckdb_libpgquery::PGNode *node) {
	auto stmt = reinterpret_cast<duckdb_libpgquery::PGAttachStmt *>(node);
	auto result = make_unique<PragmaStatement>();
	result->info->name = "attach_database";
	result->info->parameters.emplace_back(stmt->name);
	result->info->parameters.emplace_back(stmt->path);
	return result;
}

} // namespace duckdb
