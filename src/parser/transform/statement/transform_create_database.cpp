#include "duckdb/parser/statement/create_statement.hpp"
#include "duckdb/parser/parsed_data/create_database_info.hpp"
#include "duckdb/parser/transformer.hpp"
#include "duckdb/common/unordered_set.hpp"
#include "duckdb/common/operator/cast_operators.hpp"

namespace duckdb {

unique_ptr<CreateStatement> Transformer::TransformCreateDatabase(duckdb_libpgquery::PGNode *node) {
	auto stmt = reinterpret_cast<duckdb_libpgquery::PGCreateDatabaseStmt *>(node);
	auto result = make_unique<CreateStatement>();
	auto info = make_unique<CreateDatabaseInfo>();

	info->extension_name = stmt->extension ? stmt->extension : string();
	info->path = stmt->path ? stmt->path : string();

	auto qualified_name = TransformQualifiedName(stmt->name);
	if (!IsInvalidCatalog(qualified_name.catalog)) {
		throw ParserException("Expected \"CREATE DATABASE database\" ");
	}

	info->catalog = qualified_name.catalog;
	info->name = qualified_name.name;

	result->info = move(info);
	return result;
}

} // namespace duckdb
