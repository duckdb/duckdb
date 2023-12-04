#include "duckdb/catalog/catalog_entry/schema_catalog_entry.hpp"

#include "duckdb/catalog/catalog.hpp"
#include "duckdb/common/algorithm.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/catalog/dependency_list.hpp"
#include "duckdb/parser/parsed_data/create_schema_info.hpp"

#include <sstream>

namespace duckdb {

SchemaCatalogEntry::SchemaCatalogEntry(Catalog &catalog, string name_p, bool internal)
    : InCatalogEntry(CatalogType::SCHEMA_ENTRY, catalog, std::move(name_p)) {
	this->internal = internal;
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
	return std::move(result);
}

string SchemaCatalogEntry::ToSQL() const {
	std::stringstream ss;
	ss << "CREATE SCHEMA " << name << ";";
	return ss.str();
}

} // namespace duckdb
