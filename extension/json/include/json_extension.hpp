//===----------------------------------------------------------------------===//
//                         DuckDB
//
// json_extension.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb.hpp"

namespace duckdb {

class JsonExtension : public Extension {
public:
	void Load(DuckDB &db) override;
	std::string Name() override;
};

} // namespace duckdb
