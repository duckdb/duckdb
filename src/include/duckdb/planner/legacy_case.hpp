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
struct CreateInfo;

//! Normalize persisted definitions before binding so execution agrees with the on-disk format.
void NormalizeLegacyCase(Catalog &catalog, CreateInfo &info);
} // namespace duckdb
