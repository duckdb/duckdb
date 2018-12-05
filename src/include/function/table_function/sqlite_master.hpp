//===----------------------------------------------------------------------===//
//                         DuckDB
//
// function/table_function/sqlite_master.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "common/types/data_chunk.hpp"
#include "function/function.hpp"

namespace duckdb {
namespace function {

TableFunctionData *sqlite_master_init(ClientContext &);
void sqlite_master(ClientContext &, DataChunk &input, DataChunk &output, TableFunctionData *dataptr);

class SQLiteMaster {
public:
	static table_function_init_t GetInitFunction() {
		return sqlite_master_init;
	}
	static table_function_t GetFunction() {
		return sqlite_master;
	}
	static table_function_final_t GetFinalFunction() {
		return nullptr;
	}

	static const char *GetName() {
		return "sqlite_master";
	}

	static void GetArguments(vector<TypeId> &arguments) {
		// no arguments
		(void)arguments;
	}

	static void GetReturnValues(vector<ColumnDefinition> &returns) {
		returns.push_back(ColumnDefinition("type", TypeId::VARCHAR));
		returns.push_back(ColumnDefinition("name", TypeId::VARCHAR));
		returns.push_back(ColumnDefinition("tbl_name", TypeId::VARCHAR));
		returns.push_back(ColumnDefinition("rootpage", TypeId::INTEGER));
		returns.push_back(ColumnDefinition("sql", TypeId::VARCHAR));
	}
};

} // namespace function
} // namespace duckdb
