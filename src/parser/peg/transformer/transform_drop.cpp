#include "duckdb/parser/peg/transformer/peg_transformer.hpp"
#include "duckdb/parser/parsed_data/extra_drop_info.hpp"
#include "duckdb/parser/tableref/basetableref.hpp"

namespace duckdb {

unique_ptr<SQLStatement> PEGTransformerFactory::TransformDropStatement(PEGTransformer &transformer,
                                                                       unique_ptr<DropStatement> drop_entries,
                                                                       const bool &drop_behavior) {
	drop_entries->info->cascade = drop_behavior;
	return std::move(drop_entries);
}

unique_ptr<DropStatement> PEGTransformerFactory::TransformDropTable(PEGTransformer &transformer,
                                                                    const CatalogType &table_or_view,
                                                                    const bool &if_exists,
                                                                    vector<unique_ptr<BaseTableRef>> base_table_name) {
	auto result = make_uniq<DropStatement>();
	auto info = make_uniq<DropInfo>();
	if (base_table_name.size() > 1) {
		throw NotImplementedException("Can only drop one object at a time");
	}
	auto base_table = std::move(base_table_name[0]);
	info->catalog = base_table->catalog_name;
	info->schema = base_table->schema_name;
	info->name = base_table->table_name;
	info->type = table_or_view;
	info->if_not_found = if_exists ? OnEntryNotFound::RETURN_NULL : OnEntryNotFound::THROW_EXCEPTION;
	result->info = std::move(info);
	return result;
}

CatalogType PEGTransformerFactory::TransformMaterializedViewEntry(PEGTransformer &transformer) {
	throw NotImplementedException("Cannot drop MATERIALIZED VIEW yet");
}

bool PEGTransformerFactory::TransformFunctionTypeMacroKeyword(PEGTransformer &transformer) {
	return true;
}

bool PEGTransformerFactory::TransformFunctionTypeFunction(PEGTransformer &transformer) {
	return false;
}

unique_ptr<DropStatement>
PEGTransformerFactory::TransformDropTableFunction(PEGTransformer &transformer, const CatalogType &comment_macro_table,
                                                  const bool &if_exists,
                                                  const vector<Identifier> &table_function_name) {
	auto result = make_uniq<DropStatement>();
	auto info = make_uniq<DropInfo>();
	if (table_function_name.size() > 1) {
		throw NotImplementedException("Can only drop one object at a time");
	}
	info->name = table_function_name[0];
	info->catalog = INVALID_CATALOG;
	info->schema = INVALID_SCHEMA;
	info->type = comment_macro_table;
	info->if_not_found = if_exists ? OnEntryNotFound::RETURN_NULL : OnEntryNotFound::THROW_EXCEPTION;
	result->info = std::move(info);
	return result;
}

unique_ptr<DropStatement>
PEGTransformerFactory::TransformDropFunction(PEGTransformer &transformer, const bool &function_type_macro,
                                             const bool &if_exists, const vector<QualifiedName> &function_identifier) {
	auto result = make_uniq<DropStatement>();
	auto info = make_uniq<DropInfo>();
	auto catalog_type = CatalogType::MACRO_ENTRY;
	if (function_identifier.size() > 1) {
		throw NotImplementedException("Can only drop one object at a time");
	}
	const auto &function = function_identifier[0];
	info->catalog = function.catalog.empty() ? INVALID_CATALOG : function.catalog;
	info->schema = function.schema;
	info->name = function.name;
	info->if_not_found = if_exists ? OnEntryNotFound::RETURN_NULL : OnEntryNotFound::THROW_EXCEPTION;
	info->type = catalog_type;
	result->info = std::move(info);
	return result;
}

unique_ptr<DropStatement>
PEGTransformerFactory::TransformDropSchema(PEGTransformer &transformer, const bool &if_exists,
                                           const vector<QualifiedName> &qualified_schema_name) {
	auto result = make_uniq<DropStatement>();
	auto info = make_uniq<DropInfo>();
	if (qualified_schema_name.size() > 1) {
		throw NotImplementedException("Can only drop one object at a time");
	}
	const auto &schema = qualified_schema_name[0];
	info->catalog = schema.catalog;
	info->name = schema.schema;
	info->if_not_found = if_exists ? OnEntryNotFound::RETURN_NULL : OnEntryNotFound::THROW_EXCEPTION;
	info->type = CatalogType::SCHEMA_ENTRY;
	result->info = std::move(info);
	return result;
}

QualifiedName PEGTransformerFactory::TransformQualifiedSchemaNameString(PEGTransformer &transformer,
                                                                        const Identifier &schema_name) {
	QualifiedName result;
	result.catalog = INVALID_CATALOG;
	result.schema = schema_name;
	return result;
}

QualifiedName PEGTransformerFactory::TransformCatalogReservedSchema(PEGTransformer &transformer,
                                                                    const Identifier &catalog_qualification,
                                                                    const Identifier &reserved_schema_name) {
	QualifiedName result;
	result.catalog = catalog_qualification;
	result.schema = reserved_schema_name;
	return result;
}

unique_ptr<DropStatement> PEGTransformerFactory::TransformDropIndex(PEGTransformer &transformer, const bool &if_exists,
                                                                    const vector<QualifiedName> &qualified_index_name) {
	auto result = make_uniq<DropStatement>();
	auto info = make_uniq<DropInfo>();
	if (qualified_index_name.size() > 1) {
		throw NotImplementedException("Can only drop one object at a time");
	}
	const auto &index = qualified_index_name[0];
	info->catalog = index.catalog;
	info->schema = index.schema;
	info->name = index.name;
	info->type = CatalogType::INDEX_ENTRY;
	info->if_not_found = if_exists ? OnEntryNotFound::RETURN_NULL : OnEntryNotFound::THROW_EXCEPTION;
	result->info = std::move(info);
	return result;
}

QualifiedName PEGTransformerFactory::TransformQualifiedIndexNameString(PEGTransformer &transformer,
                                                                       const Identifier &index_name) {
	QualifiedName result;
	result.catalog = INVALID_CATALOG;
	result.schema = INVALID_SCHEMA;
	result.name = index_name;
	return result;
}

QualifiedName PEGTransformerFactory::TransformSchemaReservedIndex(PEGTransformer &transformer,
                                                                  const Identifier &schema_qualification,
                                                                  const Identifier &reserved_index_name) {
	QualifiedName result;
	result.catalog = INVALID_CATALOG;
	result.schema = schema_qualification;
	result.name = reserved_index_name;
	return result;
}

QualifiedName PEGTransformerFactory::TransformCatalogReservedSchemaIndex(
    PEGTransformer &transformer, const Identifier &catalog_qualification,
    const Identifier &reserved_schema_qualification, const Identifier &reserved_index_name) {
	QualifiedName result;
	result.catalog = catalog_qualification;
	result.schema = reserved_schema_qualification;
	result.name = reserved_index_name;
	return result;
}

unique_ptr<DropStatement>
PEGTransformerFactory::TransformDropSequence(PEGTransformer &transformer, const bool &if_exists,
                                             const vector<QualifiedName> &qualified_sequence_name) {
	auto result = make_uniq<DropStatement>();
	auto info = make_uniq<DropInfo>();
	if (qualified_sequence_name.size() > 1) {
		throw NotImplementedException("Can only drop one object at a time");
	}
	const auto &sequence = qualified_sequence_name[0];
	if (sequence.schema.empty()) {
		info->schema = sequence.catalog;
	} else {
		info->catalog = sequence.catalog;
		info->schema = sequence.schema;
	}
	info->name = sequence.name;
	info->if_not_found = if_exists ? OnEntryNotFound::RETURN_NULL : OnEntryNotFound::THROW_EXCEPTION;
	info->type = CatalogType::SEQUENCE_ENTRY;
	result->info = std::move(info);
	return result;
}

Identifier PEGTransformerFactory::TransformCollationName(PEGTransformer &transformer, const Identifier &identifier) {
	return identifier;
}

unique_ptr<DropStatement> PEGTransformerFactory::TransformDropCollation(PEGTransformer &transformer,
                                                                        const bool &if_exists,
                                                                        const vector<Identifier> &collation_name) {
	throw NotImplementedException("Cannot drop collation yet");
	/*
	auto result = make_uniq<DropStatement>();
	auto info = make_uniq<DropInfo>();
	if (collation_name.size() > 1) {
	    throw NotImplementedException("Can only drop one object at a time");
	}
	auto collation = collation_name[0];
	info->catalog = INVALID_CATALOG;
	info->schema = INVALID_SCHEMA;
	info->name = collation;
	info->if_not_found = if_exists ? OnEntryNotFound::RETURN_NULL : OnEntryNotFound::THROW_EXCEPTION;
	info->type = CatalogType::COLLATION_ENTRY;
	result->info = std::move(info);
	return result;
	*/
}

unique_ptr<DropStatement> PEGTransformerFactory::TransformDropType(PEGTransformer &transformer, const bool &if_exists,
                                                                   const vector<QualifiedName> &qualified_type_name) {
	auto result = make_uniq<DropStatement>();
	auto info = make_uniq<DropInfo>();
	if (qualified_type_name.size() > 1) {
		throw NotImplementedException("Can only drop one object at a time");
	}
	const auto &type = qualified_type_name[0];
	if (type.schema.empty()) {
		info->schema = type.catalog;
	} else {
		info->catalog = type.catalog;
		info->schema = type.schema;
	}
	info->name = type.name;
	info->if_not_found = if_exists ? OnEntryNotFound::RETURN_NULL : OnEntryNotFound::THROW_EXCEPTION;
	info->type = CatalogType::TYPE_ENTRY;
	result->info = std::move(info);
	return result;
}

bool PEGTransformerFactory::TransformCascadeDropBehavior(PEGTransformer &transformer) {
	return true;
}

bool PEGTransformerFactory::TransformRestrictDropBehavior(PEGTransformer &transformer) {
	return false;
}

bool PEGTransformerFactory::TransformIfExists(PEGTransformer &transformer) {
	return true;
}

unique_ptr<DropStatement> PEGTransformerFactory::TransformDropSecret(PEGTransformer &transformer,
                                                                     const SecretPersistType &temporary,
                                                                     const bool &if_exists,
                                                                     const Identifier &secret_name,
                                                                     const Identifier &drop_secret_storage) {
	auto result = make_uniq<DropStatement>();
	auto info = make_uniq<DropInfo>();
	info->type = CatalogType::SECRET_ENTRY;
	auto extra_drop_info = make_uniq<ExtraDropSecretInfo>();
	extra_drop_info->persist_mode = temporary;

	info->if_not_found = if_exists ? OnEntryNotFound::RETURN_NULL : OnEntryNotFound::THROW_EXCEPTION;
	info->name = secret_name;
	extra_drop_info->secret_storage = drop_secret_storage.GetIdentifierName();
	info->extra_drop_info = std::move(extra_drop_info);
	result->info = std::move(info);
	return result;
}

Identifier PEGTransformerFactory::TransformDropSecretStorage(PEGTransformer &transformer,
                                                             const Identifier &identifier) {
	return identifier;
}

unique_ptr<DropStatement> PEGTransformerFactory::TransformDropTrigger(PEGTransformer &transformer,
                                                                      const bool &if_exists,
                                                                      const Identifier &trigger_name,
                                                                      unique_ptr<BaseTableRef> base_table_name) {
	auto result = make_uniq<DropStatement>();
	auto info = make_uniq<DropInfo>();
	info->type = CatalogType::TRIGGER_ENTRY;
	info->if_not_found = if_exists ? OnEntryNotFound::RETURN_NULL : OnEntryNotFound::THROW_EXCEPTION;

	info->name = trigger_name;

	auto extra_info = make_uniq<ExtraDropTriggerInfo>();
	extra_info->base_table = std::move(base_table_name);
	info->extra_drop_info = std::move(extra_info);

	result->info = std::move(info);
	return result;
}

} // namespace duckdb
