#include "duckdb/catalog/similar_catalog_entry.hpp"
#include "duckdb/catalog/catalog_entry/schema_catalog_entry.hpp"
#include "duckdb/catalog/catalog.hpp"

namespace duckdb {

string SimilarCatalogEntry::GetQualifiedName(bool qualify_catalog, bool qualify_schema) const {
	D_ASSERT(Found());
	string result;
	if (qualify_catalog) {
		result += schema->catalog.GetName();
	}
	if (qualify_schema) {
		if (!result.empty()) {
			result += ".";
		}
		result += schema->name;
	}
	if (!result.empty()) {
		result += ".";
	}
	result += name;
	return result;
}

} // namespace duckdb
