#include "duckdb/parser/statement/load_statement.hpp"
#include "duckdb/parser/transformer.hpp"

namespace duckdb {

unique_ptr<LoadStatement> Transformer::TransformLoad(duckdb_libpgquery::PGNode *node) {
	D_ASSERT(node->type == duckdb_libpgquery::T_PGLoadStmt);
	auto stmt = reinterpret_cast<duckdb_libpgquery::PGLoadStmt *>(node);

	auto load_stmt = make_unique<LoadStatement>();
	auto load_info = make_unique<LoadInfo>();
	load_info->filename = std::string(stmt->filename);
	switch (stmt->load_type) {
	case duckdb_libpgquery::PG_LOAD_TYPE_LOAD:
		load_info->load_type = LoadType::LOAD;
		break;
	case duckdb_libpgquery::PG_LOAD_TYPE_INSTALL:
		load_info->load_type = LoadType::INSTALL;
		break;
	case duckdb_libpgquery::PG_LOAD_TYPE_FORCE_INSTALL:
		load_info->load_type = LoadType::FORCE_INSTALL;
		break;
	}
	load_stmt->info = std::move(load_info);
	return load_stmt;
}

} // namespace duckdb
