//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/function/udf_function.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/types.hpp"
#include "duckdb/function/scalar_function.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/parser/parsed_data/create_scalar_function_info.hpp"
#include "duckdb/common/types/data_chunk.hpp"
#include <string>
#include <vector>
#include <type_traits>
#include<tuple>

using namespace std;

namespace duckdb {

class UDFWrapper {
public:
	UDFWrapper(ClientContext &context);

	template<typename TR, typename... Args> void CreateFunction(string name, TR (*udf_func)(Args...)) {
		const std::size_t num_template_argc = sizeof...(Args);
		switch(num_template_argc) {
			case 1:
				CreateUnaryFunction<TR, Args...>(name, udf_func);
				break;
			case 2:
				CreateBinaryFunction<TR, Args...>(name, udf_func);
				break;
			case 3:
				CreateTernaryFunction<TR, Args...>(name, udf_func);
				break;
			default:
				throw duckdb::Exception(ExceptionType::EXECUTOR, "UDF function only supported until ternary!");
		}
	}

	template<typename TR, typename... Args>
	void CreateFunction(string name, vector<SQLType> args, SQLType ret_type, TR (*udf_func)(Args...)) {
		if(!TypesMatch<TR>(ret_type)) {
			string msg("Return type doesn't match with the first template type: ");
			msg += typeid(TR).name();
			msg += ".";
			throw duckdb::Exception(ExceptionType::MISMATCH_TYPE, msg);
		}

		const std::size_t num_template_types = sizeof...(Args);
		if(num_template_types != args.size()) {
			throw duckdb::Exception(ExceptionType:: MISMATCH_TYPE,
									"The number of templated types should be the same quantity of the SQLTypes (args + ret_type)!");
		}

		switch(num_template_types) {
			case 1:
				CreateUnaryFunction<TR, Args...>(name, args, ret_type, udf_func);
				break;
			case 2:
				CreateBinaryFunction<TR, Args...>(name, args, ret_type, udf_func);
				break;
			case 3:
				CreateTernaryFunction<TR, Args...>(name, args, ret_type, udf_func);
				break;
			default:
				throw duckdb::Exception(ExceptionType::EXECUTOR, "UDF function only supported until ternary!");
		}
	}

private:
	ClientContext &_context;

	//-------------------------------- Templated functions --------------------------------//
	template<typename TR, typename... Args>
	void CreateUnaryFunction(string name, TR (*udf_func)(Args...)) {
		assert(sizeof...(Args) == 1);
		CreateUnaryFunction<TR, Args...>(name, udf_func);
	}

	template<typename TR, typename TA>
	void CreateUnaryFunction(string name, TR (*udf_func)(TA)) {
	    udf_function_t udf_function = [=] (DataChunk &input, ExpressionState &state, Vector &result) -> void {
										UnaryExecutor::Execute<TA, TR>(input.data[0],
																	   result,
																	   input.size(),
																	   udf_func);
									};
	    RegisterFunction<TR, TA>(name, udf_function);
	}

	template<typename TR, typename... Args>
	void CreateBinaryFunction(string name, TR (*udf_func)(Args...)) {
		assert(sizeof...(Args) == 2);
		CreateBinaryFunction<TR, Args...>(name, udf_func);
	}

	template<typename TR, typename TA, typename TB>
	void CreateBinaryFunction(string name, TR (*udf_func)(TA, TB)) {
	    udf_function_t udf_function = [=] (DataChunk &input, ExpressionState &state, Vector &result) -> void {
										BinaryExecutor::Execute<TA, TB, TR>(input.data[0],
																		   	input.data[1],
																		    result,
																		    input.size(),
																		    udf_func);
									};
	    RegisterFunction<TR, TA, TB>(name, udf_function);
	}

	template<typename TR, typename... Args>
	void CreateTernaryFunction(string name, TR (*udf_func)(Args...)) {
		assert(sizeof...(Args) == 3);
		CreateTernaryFunction<TR, Args...>(name, udf_func);
	}

	template<typename TR, typename TA, typename TB, typename TC>
	void CreateTernaryFunction(string name, TR (*udf_func)(TA, TB, TC)) {
	    udf_function_t udf_function = [=] (DataChunk &input, ExpressionState &state, Vector &result) -> void {
										TernaryExecutor::Execute<TA, TB, TC, TR>(input.data[0],
																		   	   	 input.data[1],
																		   	   	 input.data[2],
																				 result,
																				 input.size(),
																				 udf_func);
									};
	    RegisterFunction<TR, TA, TB, TC>(name, udf_function);
	}

	template<typename TR, typename... Args>
	void RegisterFunction(string name, udf_function_t udf_function) {
	    vector<SQLType> arguments;
	    GetArgumentTypesRecursive<Args...>(arguments);

	    SQLType ret_type = GetArgumentType<TR>();

		ScalarFunction scalar_function = ScalarFunction(name, arguments, ret_type, nullptr, false,
														nullptr, nullptr, udf_function);
		CreateScalarFunctionInfo info(scalar_function);

		_context.transaction.BeginTransaction();
		_context.temporary_objects.get()->CreateFunction(_context, &info);
		_context.transaction.Commit();
	}

	template<typename T> SQLType GetArgumentType() {
		if (std::is_same<T, bool>()) {
			return SQLType::BOOLEAN;
		} else if (std::is_same<T, int8_t>()) {
			return SQLType::TINYINT;
		} else if (std::is_same<T, int16_t>()) {
			return SQLType::SMALLINT;
		} else if (std::is_same<T, int32_t>()) {
			return SQLType::INTEGER;
		} else if (std::is_same<T, int64_t>()) {
			return SQLType::BIGINT;
		} else if (std::is_same<T, float>()) {
			return SQLType::FLOAT;
		} else if (std::is_same<T, double>()) {
			return SQLType::DOUBLE;
		} else if (std::is_same<T, string_t>()) {
			return SQLType::VARCHAR;
		} else {
			// unrecognized type
			throw duckdb::Exception(ExceptionType::UNKNOWN_TYPE, "Unrecognized type!");
		}
	}

	template<typename TA, typename TB, typename... Args>
	void GetArgumentTypesRecursive(vector<SQLType> &arguments) {
		arguments.push_back(GetArgumentType<TA>());
		GetArgumentTypesRecursive<TB, Args...>(arguments);
	}

	template <typename TA>
	void GetArgumentTypesRecursive(vector<SQLType> &arguments) {
		arguments.push_back(GetArgumentType<TA>());
	}

private:
	//-------------------------------- Argumented functions --------------------------------//

	template<typename TR, typename... Args>
	void CreateUnaryFunction(string name, vector<SQLType> args, SQLType ret_type, TR (*udf_func)(Args...)) {
		assert(sizeof...(Args) == 1);
		CreateUnaryFunction<TR, Args...>(name, args, ret_type, udf_func);
	}

	template<typename TR, typename TA>
	void CreateUnaryFunction(string name, vector<SQLType> args, SQLType ret_type, TR (*udf_func)(TA)) {
		if(args.size() != 1) {
			throw duckdb::Exception(ExceptionType:: MISMATCH_TYPE,
									"The number of SQLType arguments (\"args\") should be 1!");
		}
		if(!TypesMatch<TA>(args[0])) {
			throw duckdb::Exception(ExceptionType::MISMATCH_TYPE, "The first arguments don't match!");
		}

	    udf_function_t udf_function = [=] (DataChunk &input, ExpressionState &state, Vector &result) -> void {
										UnaryExecutor::Execute<TA, TR>(input.data[0],
																	   result,
																	   input.size(),
																	   udf_func);
									};
	    RegisterFunction(name, args, ret_type, udf_function);
	}

	template<typename TR, typename... Args>
	void CreateBinaryFunction(string name, vector<SQLType> args, SQLType ret_type, TR (*udf_func)(Args...)) {
		assert(sizeof...(Args) == 2);
		CreateBinaryFunction<TR, Args...>(name, args, ret_type, udf_func);
	}

	template<typename TR, typename TA, typename TB>
	void CreateBinaryFunction(string name, vector<SQLType> args, SQLType ret_type, TR (*udf_func)(TA, TB)) {
		if(args.size() != 2) {
			throw duckdb::Exception(ExceptionType:: MISMATCH_TYPE,
									"The number of SQLType arguments (\"args\") should be 2!");
		}
		if(!TypesMatch<TA>(args[0])) {
			throw duckdb::Exception(ExceptionType::MISMATCH_TYPE, "The first arguments don't match!");
		}
		if(!TypesMatch<TB>(args[1])) {
			throw duckdb::Exception(ExceptionType::MISMATCH_TYPE, "The second arguments don't match!");
		}

		udf_function_t udf_function = [=] (DataChunk &input, ExpressionState &state, Vector &result) {
										BinaryExecutor::Execute<TA, TB, TR>(input.data[0],
																			input.data[1],
																			result,
																			input.size(),
																			udf_func);
									};
		RegisterFunction(name, args, ret_type, udf_function);
	}

	template<typename TR, typename... Args>
	void CreateTernaryFunction(string name, vector<SQLType> args, SQLType ret_type, TR (*udf_func)(Args...)) {
		assert(sizeof...(Args) == 3);
		CreateTernaryFunction<TR, Args...>(name, args, ret_type, udf_func);
	}

	template<typename TR, typename TA, typename TB, typename TC>
	void CreateTernaryFunction(string name, vector<SQLType> args, SQLType ret_type, TR (*udf_func)(TA, TB, TC)) {
		if(args.size() != 3) {
			throw duckdb::Exception(ExceptionType:: MISMATCH_TYPE,
									"The number of SQLType arguments (\"args\") should be 3!");
		}
		if(!TypesMatch<TA>(args[0])) {
			throw duckdb::Exception(ExceptionType::MISMATCH_TYPE, "The first arguments don't match!");
		}
		if(!TypesMatch<TB>(args[1])) {
			throw duckdb::Exception(ExceptionType::MISMATCH_TYPE, "The second arguments don't match!");
		}
		if(!TypesMatch<TC>(args[2])) {
			throw duckdb::Exception(ExceptionType::MISMATCH_TYPE, "The third arguments don't match!");
		}

	    udf_function_t udf_function = [=] (DataChunk &input, ExpressionState &state, Vector &result) -> void {
										TernaryExecutor::Execute<TA, TB, TC, TR>(input.data[0],
																		   	   	 input.data[1],
																		   	   	 input.data[2],
																				 result,
																				 input.size(),
																				 udf_func);
									};
		RegisterFunction(name, args, ret_type, udf_function);
	}

	void RegisterFunction(string name, vector<SQLType> args, SQLType ret_type, udf_function_t udf_function) {
		ScalarFunction scalar_function = ScalarFunction(name, args, ret_type, nullptr, false,
														nullptr, nullptr, udf_function);
		CreateScalarFunctionInfo info(scalar_function);

		_context.transaction.BeginTransaction();
		_context.temporary_objects.get()->CreateFunction(_context, &info);
		_context.transaction.Commit();
	}

	template <typename T> bool TypesMatch(SQLType sql_type) {
		switch(sql_type.id) {
		case SQLTypeId::BOOLEAN:
			return std::is_same<T, bool>();
		case SQLTypeId::TINYINT:
			return std::is_same<T, int8_t>();
		case SQLTypeId::SMALLINT:
			return std::is_same<T, int16_t>();
		case SQLTypeId::DATE:
		case SQLTypeId::TIME:
		case SQLTypeId::INTEGER:
			return std::is_same<T, int32_t>();
		case SQLTypeId::BIGINT:
		case SQLTypeId::TIMESTAMP:
			return std::is_same<T, int64_t>();
		case SQLTypeId::FLOAT:
			return std::is_same<T, float>();
		case SQLTypeId::DOUBLE:
		case SQLTypeId::DECIMAL:
			return std::is_same<T, double>();
		case SQLTypeId::VARCHAR:
		case SQLTypeId::CHAR:
		case SQLTypeId::BLOB:
			return std::is_same<T, string_t>();
		case SQLTypeId::VARBINARY:
			return std::is_same<T, blob_t>();
		default:
			throw InvalidTypeException(GetInternalType(sql_type), "Type does not supported!");
		}
	}

}; // end UDFWrapper

} // namespace duckdb
