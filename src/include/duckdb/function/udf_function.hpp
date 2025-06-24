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

namespace duckdb {

// NOLINTBEGIN

struct UDFWrapper {
public:
	template <typename TR, typename... ARGS>
	inline static scalar_function_t CreateScalarFunction(const string &name, TR (*udf_func)(ARGS...)) {
		const std::size_t num_template_argc = sizeof...(ARGS);
		switch (num_template_argc) {
		case 1:
			return CreateUnaryFunction<TR, ARGS...>(name, udf_func);
		case 2:
			return CreateBinaryFunction<TR, ARGS...>(name, udf_func);
		case 3:
			return CreateTernaryFunction<TR, ARGS...>(name, udf_func);
		default: // LCOV_EXCL_START
			throw std::runtime_error("UDF function only supported until ternary!");
		} // LCOV_EXCL_STOP
	}

	template <typename TR, typename... ARGS>
	inline static scalar_function_t CreateScalarFunction(const string &name, const vector<LogicalType> &args,
	                                                     const LogicalType &ret_type, TR (*udf_func)(ARGS...)) {
		if (!TypesMatch<TR>(ret_type)) { // LCOV_EXCL_START
			throw std::runtime_error("Return type doesn't match with the first template type.");
		} // LCOV_EXCL_STOP

		const std::size_t num_template_types = sizeof...(ARGS);
		if (num_template_types != args.size()) { // LCOV_EXCL_START
			throw std::runtime_error(
			    "The number of templated types should be the same quantity of the LogicalType arguments.");
		} // LCOV_EXCL_STOP

		switch (num_template_types) {
		case 1:
			return CreateUnaryFunction<TR, ARGS...>(name, args, ret_type, udf_func);
		case 2:
			return CreateBinaryFunction<TR, ARGS...>(name, args, ret_type, udf_func);
		case 3:
			return CreateTernaryFunction<TR, ARGS...>(name, args, ret_type, udf_func);
		default: // LCOV_EXCL_START
			throw std::runtime_error("UDF function only supported until ternary!");
		} // LCOV_EXCL_STOP
	}

	template <typename TR, typename... ARGS>
	inline static void RegisterFunction(const string &name, scalar_function_t udf_function, ClientContext &context,
	                                    LogicalType varargs = LogicalType(LogicalTypeId::INVALID)) {
		vector<LogicalType> arguments;
		GetArgumentTypesRecursive<ARGS...>(arguments);

		LogicalType ret_type = GetArgumentType<TR>();

		RegisterFunction(name, arguments, ret_type, std::move(udf_function), context, std::move(varargs));
	}

	static void RegisterFunction(string name, vector<LogicalType> args, LogicalType ret_type,
	                             scalar_function_t udf_function, ClientContext &context,
	                             LogicalType varargs = LogicalType(LogicalTypeId::INVALID));

	//--------------------------------- Aggregate UDFs ------------------------------------//
	template <typename UDF_OP, typename STATE, typename TR, typename TA>
	inline static AggregateFunction CreateAggregateFunction(const string &name) {
		return CreateUnaryAggregateFunction<UDF_OP, STATE, TR, TA>(name);
	}

	template <typename UDF_OP, typename STATE, typename TR, typename TA, typename TB>
	inline static AggregateFunction CreateAggregateFunction(const string &name) {
		return CreateBinaryAggregateFunction<UDF_OP, STATE, TR, TA, TB>(name);
	}

	template <typename UDF_OP, typename STATE, typename TR, typename TA>
	inline static AggregateFunction CreateAggregateFunction(const string &name, const LogicalType &ret_type,
	                                                        const LogicalType &input_type) {
		if (!TypesMatch<TR>(ret_type)) { // LCOV_EXCL_START
			throw std::runtime_error("The return argument don't match!");
		} // LCOV_EXCL_STOP

		if (!TypesMatch<TA>(input_type)) { // LCOV_EXCL_START
			throw std::runtime_error("The input argument don't match!");
		} // LCOV_EXCL_STOP

		return CreateUnaryAggregateFunction<UDF_OP, STATE, TR, TA>(name, ret_type, input_type);
	}

	template <typename UDF_OP, typename STATE, typename TR, typename TA, typename TB>
	inline static AggregateFunction CreateAggregateFunction(const string &name, const LogicalType &ret_type,
	                                                        const LogicalType &input_type_a,
	                                                        const LogicalType &input_type_b) {
		if (!TypesMatch<TR>(ret_type)) { // LCOV_EXCL_START
			throw std::runtime_error("The return argument don't match!");
		}

		if (!TypesMatch<TA>(input_type_a)) {
			throw std::runtime_error("The first input argument don't match!");
		}

		if (!TypesMatch<TB>(input_type_b)) {
			throw std::runtime_error("The second input argument don't match!");
		} // LCOV_EXCL_STOP

		return CreateBinaryAggregateFunction<UDF_OP, STATE, TR, TA, TB>(name, ret_type, input_type_a, input_type_b);
	}

	//! A generic CreateAggregateFunction ---------------------------------------------------------------------------//
	inline static AggregateFunction
	CreateAggregateFunction(const string &name, const vector<LogicalType> &arguments, const LogicalType &return_type,
	                        aggregate_size_t state_size, aggregate_initialize_t initialize, aggregate_update_t update,
	                        aggregate_combine_t combine, aggregate_finalize_t finalize,
	                        aggregate_simple_update_t simple_update = nullptr, bind_aggregate_function_t bind = nullptr,
	                        aggregate_destructor_t destructor = nullptr) {

		AggregateFunction aggr_function(name, arguments, return_type, state_size, initialize, update, combine, finalize,
		                                simple_update, bind, destructor);
		aggr_function.null_handling = FunctionNullHandling::SPECIAL_HANDLING;
		return aggr_function;
	}

	static void RegisterAggrFunction(AggregateFunction aggr_function, ClientContext &context,
	                                 LogicalType varargs = LogicalType(LogicalTypeId::INVALID));

private:
	//-------------------------------- Templated functions --------------------------------//
	struct UnaryUDFExecutor {
		template <class INPUT_TYPE, class RESULT_TYPE>
		static RESULT_TYPE Operation(INPUT_TYPE input, ValidityMask &mask, idx_t idx, void *dataptr) {
			typedef RESULT_TYPE (*unary_function_t)(INPUT_TYPE);
			auto udf = (unary_function_t)dataptr;
			return udf(input);
		}
	};

	template <typename TR, typename TA>
	inline static scalar_function_t CreateUnaryFunction(const string &name, TR (*udf_func)(TA)) {
		scalar_function_t udf_function = [=](DataChunk &input, ExpressionState &state, Vector &result) -> void {
			UnaryExecutor::GenericExecute<TA, TR, UnaryUDFExecutor>(input.data[0], result, input.size(),
			                                                        (void *)udf_func);
		};
		return udf_function;
	}

	template <typename TR, typename TA, typename TB>
	inline static scalar_function_t CreateBinaryFunction(const string &name, TR (*udf_func)(TA, TB)) {
		scalar_function_t udf_function = [=](DataChunk &input, ExpressionState &state, Vector &result) -> void {
			BinaryExecutor::Execute<TA, TB, TR>(input.data[0], input.data[1], result, input.size(), udf_func);
		};
		return udf_function;
	}

	template <typename TR, typename TA, typename TB, typename TC>
	inline static scalar_function_t CreateTernaryFunction(const string &name, TR (*udf_func)(TA, TB, TC)) {
		scalar_function_t udf_function = [=](DataChunk &input, ExpressionState &state, Vector &result) -> void {
			TernaryExecutor::Execute<TA, TB, TC, TR>(input.data[0], input.data[1], input.data[2], result, input.size(),
			                                         udf_func);
		};
		return udf_function;
	}

	template <typename TR, typename... ARGS>
	inline static scalar_function_t CreateUnaryFunction(const string &name,
	                                                    TR (*udf_func)(ARGS...)) { // LCOV_EXCL_START
		throw std::runtime_error("Incorrect number of arguments for unary function");
	} // LCOV_EXCL_STOP

	template <typename TR, typename... ARGS>
	inline static scalar_function_t CreateBinaryFunction(const string &name,
	                                                     TR (*udf_func)(ARGS...)) { // LCOV_EXCL_START
		throw std::runtime_error("Incorrect number of arguments for binary function");
	} // LCOV_EXCL_STOP

	template <typename TR, typename... ARGS>
	inline static scalar_function_t CreateTernaryFunction(const string &name,
	                                                      TR (*udf_func)(ARGS...)) { // LCOV_EXCL_START
		throw std::runtime_error("Incorrect number of arguments for ternary function");
	} // LCOV_EXCL_STOP

	template <typename T>
	inline static LogicalType GetArgumentType() {
		if (std::is_same<T, bool>()) {
			return LogicalType(LogicalTypeId::BOOLEAN);
		} else if (std::is_same<T, int8_t>()) {
			return LogicalType(LogicalTypeId::TINYINT);
		} else if (std::is_same<T, int16_t>()) {
			return LogicalType(LogicalTypeId::SMALLINT);
		} else if (std::is_same<T, int32_t>()) {
			return LogicalType(LogicalTypeId::INTEGER);
		} else if (std::is_same<T, int64_t>()) {
			return LogicalType(LogicalTypeId::BIGINT);
		} else if (std::is_same<T, float>()) {
			return LogicalType(LogicalTypeId::FLOAT);
		} else if (std::is_same<T, double>()) {
			return LogicalType(LogicalTypeId::DOUBLE);
		} else if (std::is_same<T, string_t>()) {
			return LogicalType(LogicalTypeId::VARCHAR);
		} else { // LCOV_EXCL_START
			throw std::runtime_error("Unrecognized type!");
		} // LCOV_EXCL_STOP
	}

	template <typename TA, typename TB, typename... ARGS>
	inline static void GetArgumentTypesRecursive(vector<LogicalType> &arguments) {
		arguments.push_back(GetArgumentType<TA>());
		GetArgumentTypesRecursive<TB, ARGS...>(arguments);
	}

	template <typename TA>
	inline static void GetArgumentTypesRecursive(vector<LogicalType> &arguments) {
		arguments.push_back(GetArgumentType<TA>());
	}

private:
	//-------------------------------- Argumented functions --------------------------------//

	template <typename TR, typename... ARGS>
	inline static scalar_function_t CreateUnaryFunction(const string &name, const vector<LogicalType> &args,
	                                                    const LogicalType &ret_type,
	                                                    TR (*udf_func)(ARGS...)) { // LCOV_EXCL_START
		throw std::runtime_error("Incorrect number of arguments for unary function");
	} // LCOV_EXCL_STOP

	template <typename TR, typename TA>
	inline static scalar_function_t CreateUnaryFunction(const string &name, const vector<LogicalType> &args,
	                                                    const LogicalType &ret_type, TR (*udf_func)(TA)) {
		if (args.size() != 1) { // LCOV_EXCL_START
			throw std::runtime_error("The number of LogicalType arguments (\"args\") should be 1!");
		}
		if (!TypesMatch<TA>(args[0])) {
			throw std::runtime_error("The first arguments don't match!");
		} // LCOV_EXCL_STOP

		scalar_function_t udf_function = [=](DataChunk &input, ExpressionState &state, Vector &result) -> void {
			UnaryExecutor::GenericExecute<TA, TR, UnaryUDFExecutor>(input.data[0], result, input.size(),
			                                                        (void *)udf_func);
		};
		return udf_function;
	}

	template <typename TR, typename... ARGS>
	inline static scalar_function_t CreateBinaryFunction(const string &name, const vector<LogicalType> &args,
	                                                     const LogicalType &ret_type,
	                                                     TR (*udf_func)(ARGS...)) { // LCOV_EXCL_START
		throw std::runtime_error("Incorrect number of arguments for binary function");
	} // LCOV_EXCL_STOP

	template <typename TR, typename TA, typename TB>
	inline static scalar_function_t CreateBinaryFunction(const string &name, const vector<LogicalType> &args,
	                                                     const LogicalType &ret_type, TR (*udf_func)(TA, TB)) {
		if (args.size() != 2) { // LCOV_EXCL_START
			throw std::runtime_error("The number of LogicalType arguments (\"args\") should be 2!");
		}
		if (!TypesMatch<TA>(args[0])) {
			throw std::runtime_error("The first arguments don't match!");
		}
		if (!TypesMatch<TB>(args[1])) {
			throw std::runtime_error("The second arguments don't match!");
		} // LCOV_EXCL_STOP

		scalar_function_t udf_function = [=](DataChunk &input, ExpressionState &state, Vector &result) {
			BinaryExecutor::Execute<TA, TB, TR>(input.data[0], input.data[1], result, input.size(), udf_func);
		};
		return udf_function;
	}

	template <typename TR, typename... ARGS>
	inline static scalar_function_t CreateTernaryFunction(const string &name, const vector<LogicalType> &args,
	                                                      const LogicalType &ret_type,
	                                                      TR (*udf_func)(ARGS...)) { // LCOV_EXCL_START
		throw std::runtime_error("Incorrect number of arguments for ternary function");
	} // LCOV_EXCL_STOP

	template <typename TR, typename TA, typename TB, typename TC>
	inline static scalar_function_t CreateTernaryFunction(const string &name, const vector<LogicalType> &args,
	                                                      const LogicalType &ret_type, TR (*udf_func)(TA, TB, TC)) {
		if (args.size() != 3) { // LCOV_EXCL_START
			throw std::runtime_error("The number of LogicalType arguments (\"args\") should be 3!");
		}
		if (!TypesMatch<TA>(args[0])) {
			throw std::runtime_error("The first arguments don't match!");
		}
		if (!TypesMatch<TB>(args[1])) {
			throw std::runtime_error("The second arguments don't match!");
		}
		if (!TypesMatch<TC>(args[2])) {
			throw std::runtime_error("The second arguments don't match!");
		} // LCOV_EXCL_STOP

		scalar_function_t udf_function = [=](DataChunk &input, ExpressionState &state, Vector &result) -> void {
			TernaryExecutor::Execute<TA, TB, TC, TR>(input.data[0], input.data[1], input.data[2], result, input.size(),
			                                         udf_func);
		};
		return udf_function;
	}

	template <typename T>
	inline static bool TypesMatch(const LogicalType &sql_type) {
		switch (sql_type.id()) {
		case LogicalTypeId::BOOLEAN:
			return std::is_same<T, bool>();
		case LogicalTypeId::TINYINT:
			return std::is_same<T, int8_t>();
		case LogicalTypeId::SMALLINT:
			return std::is_same<T, int16_t>();
		case LogicalTypeId::INTEGER:
			return std::is_same<T, int32_t>();
		case LogicalTypeId::BIGINT:
			return std::is_same<T, int64_t>();
		case LogicalTypeId::DATE:
			return std::is_same<T, date_t>();
		case LogicalTypeId::TIME:
			return std::is_same<T, dtime_t>();
		case LogicalTypeId::TIME_TZ:
			return std::is_same<T, dtime_tz_t>();
		case LogicalTypeId::TIMESTAMP:
		case LogicalTypeId::TIMESTAMP_MS:
		case LogicalTypeId::TIMESTAMP_NS:
		case LogicalTypeId::TIMESTAMP_SEC:
		case LogicalTypeId::TIMESTAMP_TZ:
			return std::is_same<T, timestamp_t>();
		case LogicalTypeId::FLOAT:
			return std::is_same<T, float>();
		case LogicalTypeId::DOUBLE:
			return std::is_same<T, double>();
		case LogicalTypeId::VARCHAR:
		case LogicalTypeId::CHAR:
		case LogicalTypeId::BLOB:
			return std::is_same<T, string_t>();
		default: // LCOV_EXCL_START
			throw std::runtime_error("Type is not supported!");
		} // LCOV_EXCL_STOP
	}

private:
	//-------------------------------- Aggregate functions --------------------------------//
	template <typename UDF_OP, typename STATE, typename TR, typename TA>
	inline static AggregateFunction CreateUnaryAggregateFunction(const string &name) {
		LogicalType return_type = GetArgumentType<TR>();
		LogicalType input_type = GetArgumentType<TA>();
		return CreateUnaryAggregateFunction<UDF_OP, STATE, TR, TA>(name, return_type, input_type);
	}

	template <typename UDF_OP, typename STATE, typename TR, typename TA>
	inline static AggregateFunction CreateUnaryAggregateFunction(const string &name, const LogicalType &ret_type,
	                                                             const LogicalType &input_type) {
		AggregateFunction aggr_function =
		    AggregateFunction::UnaryAggregate<STATE, TR, TA, UDF_OP>(input_type, ret_type);
		aggr_function.name = name;
		return aggr_function;
	}

	template <typename UDF_OP, typename STATE, typename TR, typename TA, typename TB>
	inline static AggregateFunction CreateBinaryAggregateFunction(const string &name) {
		LogicalType return_type = GetArgumentType<TR>();
		LogicalType input_type_a = GetArgumentType<TA>();
		LogicalType input_type_b = GetArgumentType<TB>();
		return CreateBinaryAggregateFunction<UDF_OP, STATE, TR, TA, TB>(name, return_type, input_type_a, input_type_b);
	}

	template <typename UDF_OP, typename STATE, typename TR, typename TA, typename TB>
	inline static AggregateFunction CreateBinaryAggregateFunction(const string &name, const LogicalType &ret_type,
	                                                              const LogicalType &input_type_a,
	                                                              const LogicalType &input_type_b) {
		AggregateFunction aggr_function =
		    AggregateFunction::BinaryAggregate<STATE, TA, TB, TR, UDF_OP>(input_type_a, input_type_b, ret_type);
		aggr_function.name = name;
		return aggr_function;
	}
}; // end UDFWrapper

// NOLINTEND

} // namespace duckdb
