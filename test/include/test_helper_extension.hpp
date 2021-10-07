//===----------------------------------------------------------------------===//
//                         DuckDB
//
// test/include/test_helper_extension.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/main/extension.hpp"

namespace duckdb {

class TestHelperExtension : public Extension {
public:
	void Load(DuckDB &db) override;
};

} // namespace duckdb