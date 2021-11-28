//===----------------------------------------------------------------------===//
//                         DuckDB
//
// icu-extension.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb.hpp"

namespace duckdb {

class ICUExtension : public Extension {
public:
	void Load(DuckDB &db) override;

private:
	void RegisterDatePartFunctions(ClientContext &context);
};

} // namespace duckdb
