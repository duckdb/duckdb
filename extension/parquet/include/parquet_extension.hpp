//===----------------------------------------------------------------------===//
//                         DuckDB
//
// parquet_extension.hpp
//
//
//===----------------------------------------------------------------------===/

#pragma once

#include <string>

#include "duckdb.hpp"
#include "duckdb/main/extension.hpp"

namespace duckdb {
class ExtensionLoader;

class ParquetExtension : public Extension {
public:
	void Load(ExtensionLoader &loader) override;
	std::string Name() override;
	std::string Version() const override;
};

} // namespace duckdb
