//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/catalog/similar_catalog_entry.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/common.hpp"
#include "duckdb/common/optional_ptr.hpp"

namespace duckdb {
class SchemaCatalogEntry;

//! Return value of SimilarEntryInSchemas
struct SimilarCatalogEntry {
	//! The entry name. Empty if absent
	string name;
	//! The distance to the given name.
	idx_t distance = idx_t(-1);
	//! The schema of the entry.
	optional_ptr<SchemaCatalogEntry> schema;

	bool Found() const {
		return !name.empty();
	}

	DUCKDB_API string GetQualifiedName(bool qualify_catalog, bool qualify_schema) const;
};

} // namespace duckdb
