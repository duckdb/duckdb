//===----------------------------------------------------------------------===//
//                         DuckDB
//
// core_functions_extension.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb.hpp"

namespace duckdb {

class CoreFunctionsExtension : public Extension {
public:
	static void RegisterFunctions(Catalog &catalog, CatalogTransaction transaction);
	void Load(DuckDB &db) override;
	std::string Name() override;
};

} // namespace duckdb
