//===----------------------------------------------------------------------===//
//
//                         DuckDB
//
// function/table_function/pragma_table_info.hpp
//
// Author: Mark Raasveldt
//
//===----------------------------------------------------------------------===//

#pragma once

#include "common/types/data_chunk.hpp"
#include "function/function.hpp"

namespace duckdb {
namespace function {

void pragma_table_info_init(ClientContext &, void **dataptr);
void pragma_table_info(ClientContext &, DataChunk &input, DataChunk &output,
                       void **dataptr);

class PragmaTableInfo {
  public:
	static table_function_init_t GetInitFunction() {
		return pragma_table_info_init;
	}
	static table_function_t GetFunction() { return pragma_table_info; }
	static table_function_final_t GetFinalFunction() { return nullptr; }

	static const char *GetName() { return "pragma_table_info"; }

	static void GetArguments(std::vector<TypeId> &arguments) {
		arguments.push_back(TypeId::VARCHAR);
	}
	static void GetReturnValues(std::vector<ColumnDefinition> &returns) {
		returns.push_back(ColumnDefinition("cid", TypeId::INTEGER));
		returns.push_back(ColumnDefinition("name", TypeId::VARCHAR));
		returns.push_back(ColumnDefinition("type", TypeId::VARCHAR));
		returns.push_back(ColumnDefinition("notnull", TypeId::BOOLEAN));
		returns.push_back(ColumnDefinition("dflt_value", TypeId::BOOLEAN));
		returns.push_back(ColumnDefinition("pk", TypeId::BOOLEAN));
	}
};

} // namespace function
} // namespace duckdb