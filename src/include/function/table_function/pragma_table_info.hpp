//===----------------------------------------------------------------------===//
//                         DuckDB
//
// function/table_function/pragma_table_info.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "common/types/data_chunk.hpp"
#include "function/function.hpp"

namespace duckdb {

FunctionData *pragma_table_info_init(ClientContext &);
void pragma_table_info(ClientContext &, DataChunk &input, DataChunk &output, FunctionData *dataptr);

class PragmaTableInfo {
public:
	static table_function_init_t GetInitFunction() {
		return pragma_table_info_init;
	}
	static table_function_t GetFunction() {
		return pragma_table_info;
	}
	static table_function_final_t GetFinalFunction() {
		return nullptr;
	}

	static const char *GetName() {
		return "pragma_table_info";
	}

	static void GetArguments(vector<SQLType> &arguments) {
		arguments.push_back(SQLType(SQLTypeId::VARCHAR));
	}
	static void GetReturnValues(vector<ColumnDefinition> &returns) {
		returns.push_back(ColumnDefinition("cid", SQLType(SQLTypeId::INTEGER)));
		returns.push_back(ColumnDefinition("name", SQLType(SQLTypeId::VARCHAR)));
		returns.push_back(ColumnDefinition("type", SQLType(SQLTypeId::VARCHAR)));
		returns.push_back(ColumnDefinition("notnull", SQLType(SQLTypeId::BOOLEAN)));
		returns.push_back(ColumnDefinition("dflt_value", SQLType(SQLTypeId::VARCHAR)));
		returns.push_back(ColumnDefinition("pk", SQLType(SQLTypeId::BOOLEAN)));
	}
};

} // namespace duckdb
