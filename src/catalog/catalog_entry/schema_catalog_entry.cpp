#include "duckdb/catalog/catalog_entry/schema_catalog_entry.hpp"

#include "duckdb/catalog/catalog.hpp"
#include "duckdb/common/algorithm.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/catalog/dependency_list.hpp"
#include "duckdb/parser/parsed_data/create_schema_info.hpp"

#include <sstream>

namespace duckdb {

SchemaCatalogEntry::SchemaCatalogEntry(Catalog &catalog, CreateSchemaInfo &info)
    : InCatalogEntry(CatalogType::SCHEMA_ENTRY, catalog, info.schema) {
	this->internal = info.internal;
	this->comment = info.comment;
}

CatalogTransaction SchemaCatalogEntry::GetCatalogTransaction(ClientContext &context) {
	return CatalogTransaction(catalog, context);
}

SimilarCatalogEntry SchemaCatalogEntry::GetSimilarEntry(CatalogTransaction transaction, CatalogType type,
                                                        const string &name) {
	SimilarCatalogEntry result;
	Scan(transaction.GetContext(), type, [&](CatalogEntry &entry) {
		auto ldist = StringUtil::SimilarityScore(entry.name, name);
		if (ldist < result.distance) {
			result.distance = ldist;
			result.name = entry.name;
		}
	});
	return result;
}

unique_ptr<CreateInfo> SchemaCatalogEntry::GetInfo() const {
	auto result = make_uniq<CreateSchemaInfo>();
	result->schema = name;
	result->comment = comment;
	return std::move(result);
}

string SchemaCatalogEntry::ToSQL() const {
	auto create_schema_info = GetInfo();
	return create_schema_info->ToString();
}

} // namespace duckdb
