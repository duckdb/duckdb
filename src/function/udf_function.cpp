#include "duckdb/function/udf_function.hpp"
#include "duckdb/common/types.hpp"


using namespace duckdb;

UDFWrapper::UDFWrapper(ClientContext &context): _context(context) {
}

void UDFWrapper::CreateFunction(string name, vector<SQLType> args, SQLType ret_type, void *udf_func) {
	// switch on return type
	switch(ret_type.id) {
	case SQLTypeId::BOOLEAN:
		CreateFunctionInitial<bool>(name, args, ret_type, udf_func);
		break;
	case SQLTypeId::TINYINT:
		CreateFunctionInitial<int8_t>(name, args, ret_type, udf_func);
		break;
	case SQLTypeId::SMALLINT:
		CreateFunctionInitial<int16_t>(name, args, ret_type, udf_func);
		break;
	case SQLTypeId::DATE:
	case SQLTypeId::TIME:
	case SQLTypeId::INTEGER:
		CreateFunctionInitial<int32_t>(name, args, ret_type, udf_func);
		break;
	case SQLTypeId::BIGINT:
	case SQLTypeId::TIMESTAMP:
		CreateFunctionInitial<int64_t>(name, args, ret_type, udf_func);
		break;
	case SQLTypeId::FLOAT:
		CreateFunctionInitial<float>(name, args, ret_type, udf_func);
		break;
	case SQLTypeId::DOUBLE:
	case SQLTypeId::DECIMAL:
		CreateFunctionInitial<double>(name, args, ret_type, udf_func);
		break;
	case SQLTypeId::VARCHAR:
	case SQLTypeId::CHAR:
	case SQLTypeId::BLOB:
		CreateFunctionInitial<string_t>(name, args, ret_type, udf_func);
		break;
	case SQLTypeId::VARBINARY:
		CreateFunctionInitial<blob_t>(name, args, ret_type, udf_func);
		break;
	default:
		throw InvalidTypeException(GetInternalType(ret_type), "Type does not supported!");
	}
}
