#include "duckdb/parser/statement/load_statement.hpp"
#include "duckdb/parser/transformer.hpp"

namespace duckdb {

unique_ptr<LoadStatement> Transformer::TransformLoad(duckdb_libpgquery::PGNode *node) {
	D_ASSERT(node->type == duckdb_libpgquery::T_PGLoadStmt);
	auto stmt = reinterpret_cast<duckdb_libpgquery::PGLoadStmt *>(node);

	auto load_stmt = make_unique<LoadStatement>();
	auto load_info = make_unique<LoadInfo>();
	load_info->filename = std::string(stmt->filename);
	load_info->install = stmt->install;
	load_stmt->info = move(load_info);
	return load_stmt;
}

} // namespace duckdb
