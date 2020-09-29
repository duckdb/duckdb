#include "duckdb/parser/statement/pragma_statement.hpp"
#include "duckdb/parser/transformer.hpp"

namespace duckdb {
using namespace std;
using namespace duckdb_libpgquery;

unique_ptr<PragmaStatement> Transformer::TransformImport(PGNode *node) {
	PGImportStmt *stmt = reinterpret_cast<PGImportStmt *>(node);
	auto result = make_unique<PragmaStatement>();
	result->info->name = "import_database";
	result->info->parameters.push_back(Value(stmt->filename));
	result->info->pragma_type = PragmaType::PRAGMA_CALL;
	return result;
}

} // namespace duckdb
