//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/catalog/dcatalog.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/catalog/catalog.hpp"

namespace duckdb {

//! The Catalog object represents the catalog of the database.
class DCatalog : public Catalog {
public:
	explicit DCatalog(AttachedDatabase &db);
	~DCatalog();

public:
	bool IsDCatalog() override;
	void Initialize(bool load_builtin) override;

	DUCKDB_API DependencyManager &GetDependencyManager() {
		return *dependency_manager;
	}
private:
	//! The DependencyManager manages dependencies between different catalog objects
	unique_ptr<DependencyManager> dependency_manager;
};

} // namespace duckdb
