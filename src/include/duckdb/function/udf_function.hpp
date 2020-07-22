//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/function/udf_function.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/function/scalar_function.hpp"

using namespace std;

namespace duckdb {

struct UDFWrapper {
public:
	template<typename TR, typename... Args>
	static scalar_function_t CreateScalarFunction(string name, TR (*udf_func)(Args...)) {
		const std::size_t num_template_argc = sizeof...(Args);
		switch(num_template_argc) {
			case 1:
				return CreateUnaryFunction<TR, Args...>(name, udf_func);
			case 2:
				return CreateBinaryFunction<TR, Args...>(name, udf_func);
			case 3:
				return CreateTernaryFunction<TR, Args...>(name, udf_func);
			default:
				throw duckdb::NotImplementedException("UDF function only supported until ternary!");
		}
	}

	template<typename TR, typename... Args>
	static scalar_function_t CreateScalarFunction(string name, vector<SQLType> args, SQLType ret_type, TR (*udf_func)(Args...)) {
		if(!TypesMatch<TR>(ret_type)) {
			throw duckdb::TypeMismatchException(GetTypeId<TR>(), GetInternalType(ret_type),
					"Return type doesn't match with the first template type.");
		}

		const std::size_t num_template_types = sizeof...(Args);
		if(num_template_types != args.size()) {
			throw duckdb::InvalidInputException("The number of templated types should be the same quantity of the SQLType arguments.");
		}

		switch(num_template_types) {
			case 1:
				return CreateUnaryFunction<TR, Args...>(name, args, ret_type, udf_func);
			case 2:
				return CreateBinaryFunction<TR, Args...>(name, args, ret_type, udf_func);
			case 3:
				return CreateTernaryFunction<TR, Args...>(name, args, ret_type, udf_func);
			default:
				throw duckdb::NotImplementedException("UDF function only supported until ternary!");
		}
	}

	template<typename TR, typename... Args>
	static void RegisterFunction(string name, scalar_function_t udf_function, ClientContext &context) {
	    vector<SQLType> arguments;
	    GetArgumentTypesRecursive<Args...>(arguments);

	    SQLType ret_type = GetArgumentType<TR>();

	    RegisterFunction(name, arguments, ret_type, udf_function, context);
	}

	static void RegisterFunction(string name, vector<SQLType> args, SQLType ret_type,
								 scalar_function_t udf_function, ClientContext &context);

private:
	//-------------------------------- Templated functions --------------------------------//
	template<typename TR, typename... Args>
	static scalar_function_t CreateUnaryFunction(string name, TR (*udf_func)(Args...)) {
		assert(sizeof...(Args) == 1);
		return CreateUnaryFunction<TR, Args...>(name, udf_func);
	}

	template<typename TR, typename TA>
	static scalar_function_t CreateUnaryFunction(string name, TR (*udf_func)(TA)) {
		scalar_function_t udf_function = [=] (DataChunk &input, ExpressionState &state, Vector &result) -> void {
										UnaryExecutor::Execute<TA, TR>(input.data[0],
																	   result,
																	   input.size(),
																	   udf_func);
									};
		return udf_function;
	}

	template<typename TR, typename... Args>
	static scalar_function_t CreateBinaryFunction(string name, TR (*udf_func)(Args...)) {
		assert(sizeof...(Args) == 2);
		return CreateBinaryFunction<TR, Args...>(name, udf_func);
	}

	template<typename TR, typename TA, typename TB>
	static scalar_function_t CreateBinaryFunction(string name, TR (*udf_func)(TA, TB)) {
		scalar_function_t udf_function = [=] (DataChunk &input, ExpressionState &state, Vector &result) -> void {
										BinaryExecutor::Execute<TA, TB, TR>(input.data[0],
																		   	input.data[1],
																		    result,
																		    input.size(),
																		    udf_func);
									};
		return udf_function;
	}

	template<typename TR, typename... Args>
	static scalar_function_t CreateTernaryFunction(string name, TR (*udf_func)(Args...)) {
		assert(sizeof...(Args) == 3);
		return CreateTernaryFunction<TR, Args...>(name, udf_func);
	}

	template<typename TR, typename TA, typename TB, typename TC>
	static scalar_function_t CreateTernaryFunction(string name, TR (*udf_func)(TA, TB, TC)) {
		scalar_function_t udf_function = [=] (DataChunk &input, ExpressionState &state, Vector &result) -> void {
										TernaryExecutor::Execute<TA, TB, TC, TR>(input.data[0],
																		   	   	 input.data[1],
																		   	   	 input.data[2],
																				 result,
																				 input.size(),
																				 udf_func);
									};
		return udf_function;
	}

	template<typename T> static SQLType GetArgumentType() {
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
			throw duckdb::InternalException("Unrecognized type!");
		}
	}

	template<typename TA, typename TB, typename... Args>
	static void GetArgumentTypesRecursive(vector<SQLType> &arguments) {
		arguments.push_back(GetArgumentType<TA>());
		GetArgumentTypesRecursive<TB, Args...>(arguments);
	}

	template <typename TA>
	static void GetArgumentTypesRecursive(vector<SQLType> &arguments) {
		arguments.push_back(GetArgumentType<TA>());
	}

private:
	//-------------------------------- Argumented functions --------------------------------//

	template<typename TR, typename... Args>
	static scalar_function_t CreateUnaryFunction(string name, vector<SQLType> args, SQLType ret_type, TR (*udf_func)(Args...)) {
		assert(sizeof...(Args) == 1);
		return CreateUnaryFunction<TR, Args...>(name, args, ret_type, udf_func);
	}

	template<typename TR, typename TA>
	static scalar_function_t CreateUnaryFunction(string name, vector<SQLType> args, SQLType ret_type, TR (*udf_func)(TA)) {
		if(args.size() != 1) {
			throw duckdb::InvalidInputException("The number of SQLType arguments (\"args\") should be 1!");
		}
		if(!TypesMatch<TA>(args[0])) {
			throw duckdb::TypeMismatchException(GetTypeId<TA>(), GetInternalType(args[0]),
					"The first arguments don't match!");
		}

		scalar_function_t udf_function = [=] (DataChunk &input, ExpressionState &state, Vector &result) -> void {
										UnaryExecutor::Execute<TA, TR>(input.data[0],
																	   result,
																	   input.size(),
																	   udf_func);
									};
		return udf_function;
	}

	template<typename TR, typename... Args>
	static scalar_function_t CreateBinaryFunction(string name, vector<SQLType> args, SQLType ret_type, TR (*udf_func)(Args...)) {
		assert(sizeof...(Args) == 2);
		return CreateBinaryFunction<TR, Args...>(name, args, ret_type, udf_func);
	}

	template<typename TR, typename TA, typename TB>
	static scalar_function_t CreateBinaryFunction(string name, vector<SQLType> args, SQLType ret_type, TR (*udf_func)(TA, TB)) {
		if(args.size() != 2) {
			throw duckdb::InvalidInputException("The number of SQLType arguments (\"args\") should be 2!");
		}
		if(!TypesMatch<TA>(args[0])) {
			throw duckdb::TypeMismatchException(GetTypeId<TA>(), GetInternalType(args[0]),
					"The first arguments don't match!");
		}
		if(!TypesMatch<TB>(args[1])) {
			throw duckdb::TypeMismatchException(GetTypeId<TB>(), GetInternalType(args[1]),
					"The second arguments don't match!");
		}

		scalar_function_t udf_function = [=] (DataChunk &input, ExpressionState &state, Vector &result) {
										BinaryExecutor::Execute<TA, TB, TR>(input.data[0],
																			input.data[1],
																			result,
																			input.size(),
																			udf_func);
									};
		return udf_function;
	}

	template<typename TR, typename... Args>
	static scalar_function_t CreateTernaryFunction(string name, vector<SQLType> args, SQLType ret_type, TR (*udf_func)(Args...)) {
		assert(sizeof...(Args) == 3);
		return CreateTernaryFunction<TR, Args...>(name, args, ret_type, udf_func);
	}

	template<typename TR, typename TA, typename TB, typename TC>
	static scalar_function_t CreateTernaryFunction(string name, vector<SQLType> args, SQLType ret_type, TR (*udf_func)(TA, TB, TC)) {
		if(args.size() != 3) {
			throw duckdb::InvalidInputException("The number of SQLType arguments (\"args\") should be 3!");
		}
		if(!TypesMatch<TA>(args[0])) {
			throw duckdb::TypeMismatchException(GetTypeId<TA>(), GetInternalType(args[0]),
					"The first arguments don't match!");
		}
		if(!TypesMatch<TB>(args[1])) {
			throw duckdb::TypeMismatchException(GetTypeId<TB>(), GetInternalType(args[1]),
					"The second arguments don't match!");
		}
		if(!TypesMatch<TC>(args[2])) {
			throw duckdb::TypeMismatchException(GetTypeId<TC>(), GetInternalType(args[2]),
					"The second arguments don't match!");
		}

		scalar_function_t udf_function = [=] (DataChunk &input, ExpressionState &state, Vector &result) -> void {
										TernaryExecutor::Execute<TA, TB, TC, TR>(input.data[0],
																		   	   	 input.data[1],
																		   	   	 input.data[2],
																				 result,
																				 input.size(),
																				 udf_func);
									};
		return udf_function;
	}

	template <typename T> static bool TypesMatch(SQLType sql_type) {
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
