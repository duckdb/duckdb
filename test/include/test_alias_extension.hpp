//===----------------------------------------------------------------------===//
//                         DuckDB
//
// test/include/test_alias_extension.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/main/extension.hpp"

namespace duckdb {

class TestAliasExtension : public Extension {
public:
	void Load(DuckDB &db) override;
	std::string Name() override;
};

} // namespace duckdb
