//===----------------------------------------------------------------------===//
//                         DuckDB
//
// json-extension.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/parser/parsed_data/create_scalar_function_info.hpp"

namespace duckdb {

class JSONFunctions {
public:
	static void AddFunctions(ClientContext &context) {
		AddExtractFunction(context);
		AddTypeFunction(context);
	}

private:
	static void AddExtractFunction(ClientContext &context);
	static void AddTypeFunction(ClientContext &context);
};

} // namespace duckdb
