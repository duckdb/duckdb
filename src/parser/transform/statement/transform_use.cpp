#include "duckdb/parser/transformer.hpp"
#include "duckdb/parser/statement/set_statement.hpp"

namespace duckdb {

unique_ptr<SetStatement> Transformer::TransformUse(duckdb_libpgquery::PGUseStmt &stmt) {
	auto qualified_name = TransformQualifiedName(*stmt.name);
	if (!IsInvalidCatalog(qualified_name.catalog)) {
		throw ParserException("Expected \"USE database\" or \"USE database.schema\"");
	}
	string name;
	if (IsInvalidSchema(qualified_name.schema)) {
		name = qualified_name.name;
	} else {
		name = qualified_name.schema + "." + qualified_name.name;
	}
	return make_uniq<SetVariableStatement>("schema", std::move(name), SetScope::AUTOMATIC);
}

} // namespace duckdb
