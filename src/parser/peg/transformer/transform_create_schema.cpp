#include "duckdb/parser/parsed_data/create_schema_info.hpp"
#include "duckdb/parser/peg/transformer/peg_transformer.hpp"

namespace duckdb {
unique_ptr<CreateStatement> PEGTransformerFactory::TransformCreateSchemaStmt(PEGTransformer &transformer,
                                                                             const optional<bool> &if_not_exists,
                                                                             const QualifiedName &qualified_name) {
	auto result = make_uniq<CreateStatement>();
	auto info = make_uniq<CreateSchemaInfo>();
	info->on_conflict = if_not_exists ? OnCreateConflict::IGNORE_ON_CONFLICT : OnCreateConflict::ERROR_ON_CONFLICT;
	// Store the full dotted path as the schema path (the new schema is the last component). The leading components
	// are resolved into a catalog + parent-schema chain during binding (see Binder::BindCreateSchema).
	auto components = qualified_name.SchemaPath();
	components.push_back(qualified_name.Name());
	info->SetQualifiedName(QualifiedName(std::move(components), Identifier()));

	result->info = std::move(info);
	return result;
}

} // namespace duckdb
