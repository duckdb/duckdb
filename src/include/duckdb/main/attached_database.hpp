//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/main/attached_database.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/common.hpp"
#include "duckdb/common/case_insensitive_map.hpp"
#include "duckdb/common/mutex.hpp"

namespace duckdb {
class Catalog;
class DatabaseInstance;

//! The AttachedDatabase represents an attached database instance
class AttachedDatabase {
public:
	explicit AttachedDatabase(DatabaseInstance &db);
	~AttachedDatabase();

private:
	unique_ptr<Catalog> catalog;
};

} // namespace duckdb
