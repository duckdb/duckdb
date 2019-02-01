//===----------------------------------------------------------------------===//
//                         DuckDB
//
// catalog/catalog_entry/prepared_statement_catalog_entry.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "catalog/catalog_entry.hpp"
#include "common/types/statistics.hpp"
#include "parser/column_definition.hpp"
#include "parser/constraint.hpp"
#include "parser/parsed_data.hpp"

#include <string>
#include <unordered_map>
#include <vector>

namespace duckdb {

//! A view catalog entry
class PreparedStatementCatalogEntry : public CatalogEntry {
public:
	//! Create a real TableCatalogEntry and initialize storage for it
	PreparedStatementCatalogEntry(string name) : CatalogEntry(CatalogType::PREPARED_STATEMENT, nullptr, name) {
	}

	// TODO fill this
};
} // namespace duckdb
