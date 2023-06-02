#include "duckdb/parser/statement/pragma_statement.hpp"
#include "duckdb/parser/transformer.hpp"

namespace duckdb {

unique_ptr<PragmaStatement> Transformer::TransformImport(duckdb_libpgquery::PGImportStmt &stmt) {
	auto result = make_uniq<PragmaStatement>();
	result->info->name = "import_database";
	result->info->parameters.emplace_back(stmt.filename);
	return result;
}

} // namespace duckdb
