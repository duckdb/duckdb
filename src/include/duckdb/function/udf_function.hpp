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

	void CreateFunction(string name, vector<SQLType> args, SQLType ret_type, void *udf_func);

private:
	ClientContext &_context;

	//-------------------------------- Templated functions --------------------------------//
	template<typename TR, typename... Args>
	void CreateUnaryFunction(string name, TR (*udf_func)(Args...)) {
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
		_context.catalog.CreateFunction(_context, &info);
		_context.transaction.Commit();
	}

	template<typename T> SQLType GetArgumentType() {
		if (std::is_same<T, int>()) {
			return SQLType::INTEGER;
		} else if (std::is_same<T, double>()) {
			return SQLType::DOUBLE;
		} else if (std::is_same<T, float>()) {
			return SQLType::FLOAT;
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

	void RegisterFunction(string name, vector<SQLType> args, SQLType ret_type, udf_function_t udf_function) {
		ScalarFunction scalar_function = ScalarFunction(name, args, ret_type, nullptr, false,
														nullptr, nullptr, udf_function);
		CreateScalarFunctionInfo info(scalar_function);

		_context.transaction.BeginTransaction();
		_context.catalog.CreateFunction(_context, &info);
		_context.transaction.Commit();
	}

	template <class TR>
	void CreateFunctionInitial(string name, vector<SQLType> args, SQLType ret_type, void *udf_func) {
		if(args.size() == 0) {
			return;
		}
		switch(args[0].id) {
		case SQLTypeId::INTEGER:
			CreateUnaryFunction<TR, int>(name, args, ret_type, udf_func);
			break;
		}
	}

	template <class TR, class TA>
	void CreateUnaryFunction(string name, vector<SQLType> args, SQLType ret_type, void *udf_func) {
		if(args.size() == 1) {
			auto func_ptr = (TR(*)(TA)) udf_func;
		    udf_function_t udf_function = [=] (DataChunk &input, ExpressionState &state, Vector &result) {
											UnaryExecutor::Execute<TA, TR>(input.data[0],
																		   result,
																		   input.size(),
																		   func_ptr);
										};
			RegisterFunction(name, args, ret_type, udf_function);
			return;
		}
		switch(args[1].id) {
		case SQLTypeId::INTEGER:
			CreateBinaryFunction<TR, TA, int>(name, args, ret_type, udf_func);
			break;
		}
	}

	template <class TR, class TA, class TB>
	void CreateBinaryFunction(string name, vector<SQLType> args, SQLType ret_type, void *udf_func) {
		if(args.size() == 2) {
			auto func_ptr = (TR(*)(TA, TB)) udf_func;
			udf_function_t udf_function = [=] (DataChunk &input, ExpressionState &state, Vector &result) {
											BinaryExecutor::Execute<TA, TB, TR>(input.data[0],
																				input.data[1],
																				result,
																				input.size(),
																				func_ptr);
										};
			RegisterFunction(name, args, ret_type, udf_function);
			return;
		}
		switch(args[2].id) {
		case SQLTypeId::INTEGER:
			CreateTernaryFunction<TR, TA, TB, int>(name, args, ret_type, udf_func);
			break;
		}
	}

	template <class TR, class TA, class TB, class TC>
	void CreateTernaryFunction(string name, vector<SQLType> args, SQLType ret_type, void *udf_func) {
		if(args.size() == 3) {
			auto func_ptr = (TR(*)(TA, TB, TC)) udf_func;
			udf_function_t udf_function = [=] (DataChunk &input, ExpressionState &state, Vector &result) {
											TernaryExecutor::Execute<TA, TB, TC, TR>(input.data[0],
																					 input.data[1],
																					 input.data[2],
																					 result,
																					 input.size(),
																					 func_ptr);
										};
			RegisterFunction(name, args, ret_type, udf_function);
			return;
		}
		throw duckdb::Exception(ExceptionType::EXECUTOR, "UDF function only supported until ternary!");
	}

}; // end UDFWrapper

} // namespace duckdb
