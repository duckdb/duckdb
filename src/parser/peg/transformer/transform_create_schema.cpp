#include "duckdb/parser/parsed_data/create_schema_info.hpp"
#include "duckdb/parser/peg/transformer/peg_transformer.hpp"

namespace duckdb {
unique_ptr<CreateStatement> PEGTransformerFactory::TransformCreateSchemaStmt(PEGTransformer &transformer,
                                                                             const optional<bool> &if_not_exists,
                                                                             const QualifiedName &qualified_name) {
	auto result = make_uniq<CreateStatement>();
	auto info = make_uniq<CreateSchemaInfo>();
	info->on_conflict = if_not_exists ? OnCreateConflict::IGNORE_ON_CONFLICT : OnCreateConflict::ERROR_ON_CONFLICT;
	// Store the full dotted path, with an empty trailing name so the new schema lands in the Schema() slot (keeping
	// catalog/schema serialization correct). The leading components are resolved into a catalog + parent-schema chain
	// during binding (see Binder::BindCreateSchema).
	info->SetQualifiedName(QualifiedName(qualified_name.Path(), Identifier()));

	result->info = std::move(info);
	return result;
}

} // namespace duckdb
