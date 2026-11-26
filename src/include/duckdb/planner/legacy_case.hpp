//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/planner/legacy_case.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

namespace duckdb {
class Catalog;
class CatalogEntry;
struct CreateInfo;
class ParsedExpression;

//! Normalize persisted definitions before binding so execution agrees with the on-disk format.
void NormalizeLegacyCase(Catalog &catalog, CreateInfo &info);
//! Use the destination format for expressions that the WAL will replay.
void NormalizeLegacyCase(CatalogEntry &entry, ParsedExpression &expression);
} // namespace duckdb
