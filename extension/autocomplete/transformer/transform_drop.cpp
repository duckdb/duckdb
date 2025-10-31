#include "transformer/peg_transformer.hpp"

namespace duckdb {

unique_ptr<SQLStatement> PEGTransformerFactory::TransformDropStatement(PEGTransformer &transformer,
                                                                       optional_ptr<ParseResult> parse_result) {
	auto &list_pr = parse_result->Cast<ListParseResult>();
	auto drop_entry = transformer.Transform<unique_ptr<DropStatement>>(list_pr.Child<ListParseResult>(1));
	transformer.TransformOptional<bool>(list_pr, 2, drop_entry->info->cascade);
	return std::move(drop_entry);
}

unique_ptr<DropStatement> PEGTransformerFactory::TransformDropEntries(PEGTransformer &transformer,
                                                                      optional_ptr<ParseResult> parse_result) {
	auto &list_pr = parse_result->Cast<ListParseResult>();
	return transformer.Transform<unique_ptr<DropStatement>>(list_pr.Child<ChoiceParseResult>(0).result);
}

unique_ptr<DropStatement> PEGTransformerFactory::TransformDropTable(PEGTransformer &transformer,
                                                                    optional_ptr<ParseResult> parse_result) {
	auto &list_pr = parse_result->Cast<ListParseResult>();
	auto result = make_uniq<DropStatement>();
	auto info = make_uniq<DropInfo>();
	auto catalog_type = transformer.Transform<CatalogType>(list_pr.Child<ListParseResult>(0));
	bool if_exists = list_pr.Child<OptionalParseResult>(1).HasResult();
	auto base_table_list = ExtractParseResultsFromList(list_pr.Child<ListParseResult>(2));
	if (base_table_list.size() > 1) {
		throw NotImplementedException("Can only drop one object at a time");
	}
	auto base_table = transformer.Transform<unique_ptr<BaseTableRef>>(base_table_list[0]);
	info->catalog = base_table->catalog_name;
	info->schema = base_table->schema_name;
	info->name = base_table->table_name;
	info->type = catalog_type;
	info->if_not_found = if_exists ? OnEntryNotFound::RETURN_NULL : OnEntryNotFound::THROW_EXCEPTION;
	result->info = std::move(info);
	return result;
}

CatalogType PEGTransformerFactory::TransformTableOrView(PEGTransformer &transformer,
                                                        optional_ptr<ParseResult> parse_result) {
	auto &list_pr = parse_result->Cast<ListParseResult>();
	return transformer.TransformEnum<CatalogType>(list_pr.Child<ChoiceParseResult>(0).result);
}

unique_ptr<DropStatement> PEGTransformerFactory::TransformDropTableFunction(PEGTransformer &transformer,
                                                                            optional_ptr<ParseResult> parse_result) {
	auto &list_pr = parse_result->Cast<ListParseResult>();
	auto result = make_uniq<DropStatement>();
	auto info = make_uniq<DropInfo>();
	auto catalog_type = transformer.TransformEnum<CatalogType>(list_pr.Child<ListParseResult>(0));
	bool if_exists = list_pr.Child<OptionalParseResult>(1).HasResult();
	auto table_function_list = ExtractParseResultsFromList(list_pr.Child<ListParseResult>(2));
	if (table_function_list.size() > 1) {
		throw NotImplementedException("Can only drop one object at a time");
	}
	info->name = table_function_list[0]->Cast<IdentifierParseResult>().identifier;
	info->catalog = INVALID_CATALOG;
	info->schema = INVALID_SCHEMA;
	info->type = catalog_type;
	info->if_not_found = if_exists ? OnEntryNotFound::RETURN_NULL : OnEntryNotFound::THROW_EXCEPTION;
	result->info = std::move(info);
	return result;
}

unique_ptr<DropStatement> PEGTransformerFactory::TransformDropFunction(PEGTransformer &transformer,
                                                                       optional_ptr<ParseResult> parse_result) {
	auto &list_pr = parse_result->Cast<ListParseResult>();
	auto result = make_uniq<DropStatement>();
	auto info = make_uniq<DropInfo>();
	auto catalog_type = CatalogType::MACRO_ENTRY;
	bool if_exists = list_pr.Child<OptionalParseResult>(1).HasResult();
	auto function_list = ExtractParseResultsFromList(list_pr.Child<ListParseResult>(2));
	if (function_list.size() > 1) {
		throw NotImplementedException("Can only drop one object at a time");
	}
	auto function = transformer.Transform<QualifiedName>(function_list[0]);
	info->catalog = function.catalog == INVALID_CATALOG ? INVALID_CATALOG : function.catalog;
	info->schema = function.schema;
	info->name = function.name;
	info->if_not_found = if_exists ? OnEntryNotFound::RETURN_NULL : OnEntryNotFound::THROW_EXCEPTION;
	info->type = catalog_type;
	result->info = std::move(info);
	return result;
}

unique_ptr<DropStatement> PEGTransformerFactory::TransformDropSchema(PEGTransformer &transformer,
                                                                     optional_ptr<ParseResult> parse_result) {
	auto &list_pr = parse_result->Cast<ListParseResult>();
	auto result = make_uniq<DropStatement>();
	auto info = make_uniq<DropInfo>();
	bool if_exists = list_pr.Child<OptionalParseResult>(1).HasResult();
	auto schema_list = ExtractParseResultsFromList(list_pr.Child<ListParseResult>(2));
	if (schema_list.size() > 1) {
		throw NotImplementedException("Can only drop one object at a time");
	}
	auto schema = transformer.Transform<QualifiedName>(schema_list[0]);
	info->catalog = schema.catalog;
	info->name = schema.schema;
	info->if_not_found = if_exists ? OnEntryNotFound::RETURN_NULL : OnEntryNotFound::THROW_EXCEPTION;
	info->type = CatalogType::SCHEMA_ENTRY;
	result->info = std::move(info);
	return result;
}

QualifiedName PEGTransformerFactory::TransformQualifiedSchemaName(PEGTransformer &transformer,
                                                                  optional_ptr<ParseResult> parse_result) {
	auto &list_pr = parse_result->Cast<ListParseResult>();
	QualifiedName result;
	string catalog = INVALID_CATALOG;
	transformer.TransformOptional<string>(list_pr, 0, catalog);
	result.catalog = catalog;
	result.schema = list_pr.Child<IdentifierParseResult>(1).identifier;
	return result;
}

unique_ptr<DropStatement> PEGTransformerFactory::TransformDropIndex(PEGTransformer &transformer,
                                                                    optional_ptr<ParseResult> parse_result) {
	auto &list_pr = parse_result->Cast<ListParseResult>();
	auto result = make_uniq<DropStatement>();
	auto info = make_uniq<DropInfo>();
	bool if_exists = list_pr.Child<OptionalParseResult>(1).HasResult();
	auto index_list = ExtractParseResultsFromList(list_pr.Child<ListParseResult>(2));
	if (index_list.size() > 1) {
		throw NotImplementedException("Can only drop one object at a time");
	}
	auto index = transformer.Transform<QualifiedName>(index_list[0]);
	info->catalog = index.catalog;
	info->schema = index.schema;
	info->name = index.name;
	info->type = CatalogType::INDEX_ENTRY;
	info->if_not_found = if_exists ? OnEntryNotFound::RETURN_NULL : OnEntryNotFound::THROW_EXCEPTION;
	result->info = std::move(info);
	return result;
}

QualifiedName PEGTransformerFactory::TransformQualifiedIndexName(PEGTransformer &transformer,
                                                                 optional_ptr<ParseResult> parse_result) {
	auto &list_pr = parse_result->Cast<ListParseResult>();
	QualifiedName result;
	result.catalog = INVALID_CATALOG;
	result.schema = INVALID_SCHEMA;
	transformer.TransformOptional<string>(list_pr, 0, result.catalog);
	transformer.TransformOptional<string>(list_pr, 1, result.schema);
	result.name = list_pr.Child<IdentifierParseResult>(2).identifier;
	return result;
}

unique_ptr<DropStatement> PEGTransformerFactory::TransformDropSequence(PEGTransformer &transformer,
                                                                       optional_ptr<ParseResult> parse_result) {
	auto &list_pr = parse_result->Cast<ListParseResult>();
	auto result = make_uniq<DropStatement>();
	auto info = make_uniq<DropInfo>();
	bool if_exists = list_pr.Child<OptionalParseResult>(1).HasResult();
	auto sequence_list = ExtractParseResultsFromList(list_pr.Child<ListParseResult>(2));
	if (sequence_list.size() > 1) {
		throw NotImplementedException("Can only drop one object at a time");
	}
	auto sequence = transformer.Transform<QualifiedName>(sequence_list[0]);
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

unique_ptr<DropStatement> PEGTransformerFactory::TransformDropCollation(PEGTransformer &transformer,
                                                                        optional_ptr<ParseResult> parse_result) {
	auto &list_pr = parse_result->Cast<ListParseResult>();
	auto result = make_uniq<DropStatement>();
	auto info = make_uniq<DropInfo>();
	bool if_exists = list_pr.Child<OptionalParseResult>(1).HasResult();
	auto collation_list = ExtractParseResultsFromList(list_pr.Child<ListParseResult>(2));
	if (collation_list.size() > 1) {
		throw NotImplementedException("Can only drop one object at a time");
	}
	auto collation = collation_list[0]->Cast<IdentifierParseResult>().identifier;
	info->catalog = INVALID_CATALOG;
	info->schema = INVALID_SCHEMA;
	info->name = collation;
	info->if_not_found = if_exists ? OnEntryNotFound::RETURN_NULL : OnEntryNotFound::THROW_EXCEPTION;
	info->type = CatalogType::SEQUENCE_ENTRY;
	result->info = std::move(info);
	return result;
}

unique_ptr<DropStatement> PEGTransformerFactory::TransformDropType(PEGTransformer &transformer,
                                                                   optional_ptr<ParseResult> parse_result) {
	auto &list_pr = parse_result->Cast<ListParseResult>();
	auto result = make_uniq<DropStatement>();
	auto info = make_uniq<DropInfo>();
	bool if_exists = list_pr.Child<OptionalParseResult>(1).HasResult();
	auto type_list = ExtractParseResultsFromList(list_pr.Child<ListParseResult>(2));
	if (type_list.size() > 1) {
		throw NotImplementedException("Can only drop one object at a time");
	}
	auto type = transformer.Transform<QualifiedName>(type_list[0]);
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

bool PEGTransformerFactory::TransformDropBehavior(PEGTransformer &transformer, optional_ptr<ParseResult> parse_result) {
	auto &list_pr = parse_result->Cast<ListParseResult>();
	auto choice_pr = list_pr.Child<ChoiceParseResult>(0).result;
	return StringUtil::CIEquals(choice_pr->Cast<KeywordParseResult>().keyword, "cascade");
}

unique_ptr<DropStatement> PEGTransformerFactory::TransformDropSecret(PEGTransformer &transformer,
                                                                     optional_ptr<ParseResult> parse_result) {
	auto &list_pr = parse_result->Cast<ListParseResult>();
	auto result = make_uniq<DropStatement>();
	auto info = make_uniq<DropInfo>();
	info->type = CatalogType::SECRET_ENTRY;
	auto extra_drop_info = make_uniq<ExtraDropSecretInfo>();
	extra_drop_info->persist_mode = SecretPersistType::DEFAULT;
	transformer.TransformOptional<SecretPersistType>(list_pr, 0, extra_drop_info->persist_mode);

	bool if_exists = list_pr.Child<OptionalParseResult>(2).HasResult();
	info->if_not_found = if_exists ? OnEntryNotFound::RETURN_NULL : OnEntryNotFound::THROW_EXCEPTION;
	info->name = transformer.Transform<string>(list_pr.Child<ListParseResult>(3));
	transformer.TransformOptional<string>(list_pr, 4, extra_drop_info->secret_storage);
	info->extra_drop_info = std::move(extra_drop_info);
	result->info = std::move(info);
	return result;
}

string PEGTransformerFactory::TransformDropSecretStorage(PEGTransformer &transformer,
                                                         optional_ptr<ParseResult> parse_result) {
	auto &list_pr = parse_result->Cast<ListParseResult>();
	return list_pr.Child<IdentifierParseResult>(1).identifier;
}

} // namespace duckdb
