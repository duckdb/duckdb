#include "duckdb/parser/parsed_data/create_schema_info.hpp"
#include "duckdb/parser/peg/transformer/peg_transformer.hpp"

namespace duckdb {
unique_ptr<CreateStatement> PEGTransformerFactory::TransformCreateSchemaStmt(PEGTransformer &transformer,
                                                                             const optional<bool> &if_not_exists,
                                                                             const QualifiedName &qualified_name) {
	if (!qualified_name.Catalog().empty()) {
		throw ParserException("CREATE SCHEMA too many dots: expected \"catalog.schema\" or \"schema\"");
	}
	auto result = make_uniq<CreateStatement>();
	auto info = make_uniq<CreateSchemaInfo>();
	info->on_conflict = if_not_exists ? OnCreateConflict::IGNORE_ON_CONFLICT : OnCreateConflict::ERROR_ON_CONFLICT;
	info->CatalogMutable() = qualified_name.Schema();
	info->SchemaMutable() = qualified_name.Name();

	result->info = std::move(info);
	return result;
}

} // namespace duckdb
