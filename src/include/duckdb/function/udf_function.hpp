//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/function/udf_function.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/function/scalar_function.hpp"
#include "duckdb/function/aggregate_function.hpp"

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
	static scalar_function_t CreateScalarFunction(string name, vector<LogicalType> args, LogicalType ret_type, TR (*udf_func)(Args...)) {
		if(!TypesMatch<TR>(ret_type)) {
			throw duckdb::TypeMismatchException(GetTypeId<TR>(), ret_type.InternalType(),
					"Return type doesn't match with the first template type.");
		}

		const std::size_t num_template_types = sizeof...(Args);
		if(num_template_types != args.size()) {
			throw duckdb::InvalidInputException("The number of templated types should be the same quantity of the LogicalType arguments.");
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
	static void RegisterFunction(string name, scalar_function_t udf_function, ClientContext &context, LogicalType varargs = LogicalType::INVALID) {
	    vector<LogicalType> arguments;
	    GetArgumentTypesRecursive<Args...>(arguments);

	    LogicalType ret_type = GetArgumentType<TR>();

	    RegisterFunction(name, arguments, ret_type, udf_function, context, varargs);
	}

	static void RegisterFunction(string name, vector<LogicalType> args, LogicalType ret_type,
								 scalar_function_t udf_function, ClientContext &context,
								 LogicalType varargs = LogicalType::INVALID);

	//--------------------------------- Aggregate UDFs ------------------------------------//
	template<typename UDF_OP, typename STATE, typename TR, typename TA>
	static AggregateFunction CreateAggregateFunction(string name) {
		return CreateUnaryAggregateFunction<UDF_OP, STATE, TR, TA>(name);
	}

	template<typename UDF_OP, typename STATE, typename TR, typename TA, typename TB>
	static AggregateFunction CreateAggregateFunction(string name) {
		return CreateBinaryAggregateFunction<UDF_OP, STATE, TR, TA, TB>(name);
	}

	template<typename UDF_OP, typename STATE, typename TR, typename TA>
	static AggregateFunction CreateAggregateFunction(string name, LogicalType ret_type, LogicalType input_type) {
		if(!TypesMatch<TR>(ret_type)) {
			throw duckdb::TypeMismatchException(GetTypeId<TR>(), ret_type.InternalType(),
					"The return argument don't match!");
		}

		if(!TypesMatch<TA>(input_type)) {
			throw duckdb::TypeMismatchException(GetTypeId<TA>(), input_type.InternalType(),
					"The input argument don't match!");
		}

		return CreateUnaryAggregateFunction<UDF_OP, STATE, TR, TA>(name, ret_type, input_type);
	}

	template<typename UDF_OP, typename STATE, typename TR, typename TA, typename TB>
	static AggregateFunction CreateAggregateFunction(string name, LogicalType ret_type, LogicalType input_typeA, LogicalType input_typeB) {
		if(!TypesMatch<TR>(ret_type)) {
			throw duckdb::TypeMismatchException(GetTypeId<TR>(), ret_type.InternalType(),
					"The return argument don't match!");
		}

		if(!TypesMatch<TA>(input_typeA)) {
			throw duckdb::TypeMismatchException(GetTypeId<TA>(), input_typeA.InternalType(),
					"The first input argument don't match!");
		}

		if(!TypesMatch<TB>(input_typeB)) {
			throw duckdb::TypeMismatchException(GetTypeId<TB>(), input_typeB.InternalType(),
					"The second input argument don't match!");
		}

		return CreateBinaryAggregateFunction<UDF_OP, STATE, TR, TA, TB>(name, ret_type, input_typeA, input_typeB);
	}

	static void RegisterAggrFunction(AggregateFunction aggr_function, ClientContext &context, LogicalType varargs = LogicalType::INVALID);

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

	template<typename T> static LogicalType GetArgumentType() {
		if (std::is_same<T, bool>()) {
			return LogicalType::BOOLEAN;
		} else if (std::is_same<T, int8_t>()) {
			return LogicalType::TINYINT;
		} else if (std::is_same<T, int16_t>()) {
			return LogicalType::SMALLINT;
		} else if (std::is_same<T, int32_t>()) {
			return LogicalType::INTEGER;
		} else if (std::is_same<T, int64_t>()) {
			return LogicalType::BIGINT;
		} else if (std::is_same<T, float>()) {
			return LogicalType::FLOAT;
		} else if (std::is_same<T, double>()) {
			return LogicalType::DOUBLE;
		} else if (std::is_same<T, string_t>()) {
			return LogicalType::VARCHAR;
		} else {
			// unrecognized type
			throw duckdb::InternalException("Unrecognized type!");
		}
	}

	template<typename TA, typename TB, typename... Args>
	static void GetArgumentTypesRecursive(vector<LogicalType> &arguments) {
		arguments.push_back(GetArgumentType<TA>());
		GetArgumentTypesRecursive<TB, Args...>(arguments);
	}

	template <typename TA>
	static void GetArgumentTypesRecursive(vector<LogicalType> &arguments) {
		arguments.push_back(GetArgumentType<TA>());
	}

private:
	//-------------------------------- Argumented functions --------------------------------//

	template<typename TR, typename... Args>
	static scalar_function_t CreateUnaryFunction(string name, vector<LogicalType> args, LogicalType ret_type, TR (*udf_func)(Args...)) {
		assert(sizeof...(Args) == 1);
		return CreateUnaryFunction<TR, Args...>(name, args, ret_type, udf_func);
	}

	template<typename TR, typename TA>
	static scalar_function_t CreateUnaryFunction(string name, vector<LogicalType> args, LogicalType ret_type, TR (*udf_func)(TA)) {
		if(args.size() != 1) {
			throw duckdb::InvalidInputException("The number of LogicalType arguments (\"args\") should be 1!");
		}
		if(!TypesMatch<TA>(args[0])) {
			throw duckdb::TypeMismatchException(GetTypeId<TA>(), args[0].InternalType(),
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
	static scalar_function_t CreateBinaryFunction(string name, vector<LogicalType> args, LogicalType ret_type, TR (*udf_func)(Args...)) {
		assert(sizeof...(Args) == 2);
		return CreateBinaryFunction<TR, Args...>(name, args, ret_type, udf_func);
	}

	template<typename TR, typename TA, typename TB>
	static scalar_function_t CreateBinaryFunction(string name, vector<LogicalType> args, LogicalType ret_type, TR (*udf_func)(TA, TB)) {
		if(args.size() != 2) {
			throw duckdb::InvalidInputException("The number of LogicalType arguments (\"args\") should be 2!");
		}
		if(!TypesMatch<TA>(args[0])) {
			throw duckdb::TypeMismatchException(GetTypeId<TA>(), args[0].InternalType(),
					"The first arguments don't match!");
		}
		if(!TypesMatch<TB>(args[1])) {
			throw duckdb::TypeMismatchException(GetTypeId<TB>(), args[1].InternalType(),
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
	static scalar_function_t CreateTernaryFunction(string name, vector<LogicalType> args, LogicalType ret_type, TR (*udf_func)(Args...)) {
		assert(sizeof...(Args) == 3);
		return CreateTernaryFunction<TR, Args...>(name, args, ret_type, udf_func);
	}

	template<typename TR, typename TA, typename TB, typename TC>
	static scalar_function_t CreateTernaryFunction(string name, vector<LogicalType> args, LogicalType ret_type, TR (*udf_func)(TA, TB, TC)) {
		if(args.size() != 3) {
			throw duckdb::InvalidInputException("The number of LogicalType arguments (\"args\") should be 3!");
		}
		if(!TypesMatch<TA>(args[0])) {
			throw duckdb::TypeMismatchException(GetTypeId<TA>(), args[0].InternalType(),
					"The first arguments don't match!");
		}
		if(!TypesMatch<TB>(args[1])) {
			throw duckdb::TypeMismatchException(GetTypeId<TB>(), args[1].InternalType(),
					"The second arguments don't match!");
		}
		if(!TypesMatch<TC>(args[2])) {
			throw duckdb::TypeMismatchException(GetTypeId<TC>(), args[2].InternalType(),
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

	template <typename T> static bool TypesMatch(LogicalType sql_type) {
		switch(sql_type.id()) {
		case LogicalTypeId::BOOLEAN:
			return std::is_same<T, bool>();
		case LogicalTypeId::TINYINT:
			return std::is_same<T, int8_t>();
		case LogicalTypeId::SMALLINT:
			return std::is_same<T, int16_t>();
		case LogicalTypeId::DATE:
		case LogicalTypeId::TIME:
		case LogicalTypeId::INTEGER:
			return std::is_same<T, int32_t>();
		case LogicalTypeId::BIGINT:
		case LogicalTypeId::TIMESTAMP:
			return std::is_same<T, int64_t>();
		case LogicalTypeId::FLOAT:
			return std::is_same<T, float>();
		case LogicalTypeId::DOUBLE:
		case LogicalTypeId::DECIMAL:
			return std::is_same<T, double>();
		case LogicalTypeId::VARCHAR:
		case LogicalTypeId::CHAR:
		case LogicalTypeId::BLOB:
			return std::is_same<T, string_t>();
		case LogicalTypeId::VARBINARY:
			return std::is_same<T, blob_t>();
		default:
			throw InvalidTypeException(sql_type.InternalType(), "Type does not supported!");
		}
	}

private:
	//-------------------------------- Aggregate functions --------------------------------//
	template<typename UDF_OP, typename STATE, typename TR, typename TA>
	static AggregateFunction CreateUnaryAggregateFunction(string name) {
		LogicalType return_type = GetArgumentType<TR>();
		LogicalType input_type = GetArgumentType<TA>();
		return CreateUnaryAggregateFunction<UDF_OP, STATE, TR, TA>(name, return_type, input_type);
	}
	template<typename UDF_OP, typename STATE, typename TR, typename TA>
	static AggregateFunction CreateUnaryAggregateFunction(string name, LogicalType ret_type, LogicalType input_type) {
		AggregateFunction aggr_function = AggregateFunction::UnaryAggregate<STATE, TR, TA, UDF_OP>(input_type, ret_type);
		aggr_function.name = name;
		return aggr_function;
	}

	template<typename UDF_OP, typename STATE, typename TR, typename TA, typename TB>
	static AggregateFunction CreateBinaryAggregateFunction(string name) {
		LogicalType return_type = GetArgumentType<TR>();
		LogicalType input_typeA = GetArgumentType<TA>();
		LogicalType input_typeB = GetArgumentType<TB>();
		return CreateBinaryAggregateFunction<UDF_OP, STATE, TR, TA, TB>(name, return_type, input_typeA, input_typeB);
	}

	template<typename UDF_OP, typename STATE, typename TR, typename TA, typename TB>
	static AggregateFunction CreateBinaryAggregateFunction(string name, LogicalType ret_type, LogicalType input_typeA, LogicalType input_typeB) {
		AggregateFunction aggr_function = AggregateFunction::BinaryAggregate<STATE, TR, TA, TB, UDF_OP>(input_typeA, input_typeB,
																									   ret_type);
		aggr_function.name = name;
		return aggr_function;
	}

}; // end UDFWrapper

} // namespace duckdb
