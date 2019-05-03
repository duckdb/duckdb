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

FunctionData *sqlite_master_init(ClientContext &);
void sqlite_master(ClientContext &, DataChunk &input, DataChunk &output, FunctionData *dataptr);

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

	static void GetArguments(vector<SQLType> &arguments) {
		// no arguments
		(void)arguments;
	}

	static void GetReturnValues(vector<ColumnDefinition> &returns) {
		returns.push_back(ColumnDefinition("type", SQLType(SQLTypeId::VARCHAR)));
		returns.push_back(ColumnDefinition("name", SQLType(SQLTypeId::VARCHAR)));
		returns.push_back(ColumnDefinition("tbl_name", SQLType(SQLTypeId::VARCHAR)));
		returns.push_back(ColumnDefinition("rootpage", SQLType(SQLTypeId::INTEGER)));
		returns.push_back(ColumnDefinition("sql", SQLType(SQLTypeId::VARCHAR)));
	}
};

} // namespace duckdb
