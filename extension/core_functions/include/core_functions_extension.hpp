//===----------------------------------------------------------------------===//
//                         DuckDB
//
// core_functions_extension.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once


#include "duckdb.hpp"
#include "duckdb/main/extension.hpp"

namespace duckdb {
class ExtensionLoader;

class CoreFunctionsExtension : public Extension {
public:
	void Load(ExtensionLoader &db) override;
	std::string Name() override;
	std::string Version() const override;
};

} // namespace duckdb
