#include "duckdb/parser/transformer.hpp"
#include "duckdb/parser/statement/set_statement.hpp"

namespace duckdb {

unique_ptr<SetStatement> Transformer::TransformUse(duckdb_libpgquery::PGNode *node) {
	auto stmt = reinterpret_cast<duckdb_libpgquery::PGUseStmt *>(node);
	auto qualified_name = TransformQualifiedName(stmt->name);
	if (!IsInvalidCatalog(qualified_name.catalog)) {
		throw ParserException("Expected \"USE database\" or \"USE database.schema\"");
	}
	string name;
	if (IsInvalidSchema(qualified_name.schema)) {
		name = qualified_name.name;
	} else {
		name = qualified_name.schema + "." + qualified_name.name;
	}
	return make_unique<SetVariableStatement>("schema", move(name), SetScope::AUTOMATIC);
}

} // namespace duckdb
