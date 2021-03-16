//===----------------------------------------------------------------------===//
//                         DuckDB
//
// simd-extension.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb.hpp"

namespace duckdb {

class SIMDExtension : public Extension {
public:
	void Load(DuckDB &db) override;
};

} // namespace duckdb
