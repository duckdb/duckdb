//===----------------------------------------------------------------------===//
//                         DuckDB
//
// inet_extension.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb.hpp"
#include "duckdb/main/client_context.hpp"

namespace duckdb {

class InetExtension : public Extension {
public:
	void Load(DuckDB &db) override;
	std::string Name() override;

protected:
	ScalarFunctionSet GetEscapeFunctionSet();
	ScalarFunction GetUnescapeFunction();
};

} // namespace duckdb
