#include "duckdb/function/udf_function.hpp"

using namespace duckdb;

UDFWrapper::UDFWrapper(ClientContext &context): _context(context) {
}

void UDFWrapper::CreateFunction(string name, vector<SQLType> args, SQLType ret_type, void *udf_func) {
	// switch on return type
	switch(ret_type.id) {
	case SQLTypeId::INTEGER:
		CreateFunctionInitial<int>(name, args, ret_type, udf_func);
		break;
	}
}
