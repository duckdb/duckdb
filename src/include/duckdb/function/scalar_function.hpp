//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/function/scalar_function.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/vector_operations/binary_executor.hpp"
#include "duckdb/common/vector_operations/ternary_executor.hpp"
#include "duckdb/common/vector_operations/unary_executor.hpp"
#include "duckdb/common/vector_operations/vector_operations.hpp"
#include "duckdb/execution/expression_executor_state.hpp"
#include "duckdb/function/function.hpp"

namespace duckdb {
class BoundFunctionExpression;
class ScalarFunctionCatalogEntry;

//! The type used for scalar functions
typedef std::function<void(DataChunk &, ExpressionState &, Vector &)> scalar_function_t;
//! Binds the scalar function and creates the function data
typedef unique_ptr<FunctionData> (*bind_scalar_function_t)(ClientContext &context, ScalarFunction &bound_function, vector<unique_ptr<Expression>> &arguments);
//! Adds the dependencies of this BoundFunctionExpression to the set of dependencies
typedef void (*dependency_function_t)(BoundFunctionExpression &expr, unordered_set<CatalogEntry *> &dependencies);

class ScalarFunction : public BaseScalarFunction {
public:
	ScalarFunction(string name, vector<LogicalType> arguments, LogicalType return_type, scalar_function_t function,
	               bool has_side_effects = false, bind_scalar_function_t bind = nullptr,
	               dependency_function_t dependency = nullptr, LogicalType varargs = LogicalType::INVALID)
	    : BaseScalarFunction(name, arguments, return_type, has_side_effects, varargs), function(function), bind(bind),
	      dependency(dependency) {
	}

	ScalarFunction(vector<LogicalType> arguments, LogicalType return_type, scalar_function_t function,
	               bool has_side_effects = false, bind_scalar_function_t bind = nullptr,
	               dependency_function_t dependency = nullptr, LogicalType varargs = LogicalType::INVALID)
	    : ScalarFunction(string(), arguments, return_type, function, has_side_effects, bind, dependency, varargs) {
	}

	//! The main scalar function to execute
	scalar_function_t function;
	//! The bind function (if any)
	bind_scalar_function_t bind;
	// The dependency function (if any)
	dependency_function_t dependency;

	static unique_ptr<BoundFunctionExpression> BindScalarFunction(ClientContext &context, string schema, string name,
	                                                              vector<unique_ptr<Expression>> children,
	                                                              bool is_operator = false);
	static unique_ptr<BoundFunctionExpression>
	BindScalarFunction(ClientContext &context, ScalarFunctionCatalogEntry &function,
	                   vector<unique_ptr<Expression>> children, bool is_operator = false);


	static unique_ptr<BoundFunctionExpression> BindScalarFunction(ClientContext &context, ScalarFunction bound_function,
                                                                  vector<unique_ptr<Expression>> children,
	                                                              bool is_operator = false);

	bool operator==(const ScalarFunction &rhs) const {
		return CompareScalarFunctionT(rhs.function) && bind == rhs.bind && dependency == rhs.dependency;
	}
	bool operator!=(const ScalarFunction &rhs) const {
		return !(*this == rhs);
	}

private:
	bool CompareScalarFunctionT(const scalar_function_t other) const {
		typedef void(funcTypeT)(DataChunk &, ExpressionState &, Vector &);

		funcTypeT **func_ptr = (funcTypeT **)function.template target<funcTypeT *>();
		funcTypeT **other_ptr = (funcTypeT **)other.template target<funcTypeT *>();

		// Case the functions were created from lambdas the target will return a nullptr
		if (func_ptr == nullptr || other_ptr == nullptr) {
			// scalar_function_t (std::functions) from lambdas cannot be compared
			return false;
		}
		return ((size_t)*func_ptr == (size_t)*other_ptr);
	}

public:
	static void NopFunction(DataChunk &input, ExpressionState &state, Vector &result) {
		assert(input.column_count() >= 1);
		result.Reference(input.data[0]);
	};

	template <class TA, class TR, class OP, bool SKIP_NULLS = false>
	static void UnaryFunction(DataChunk &input, ExpressionState &state, Vector &result) {
		assert(input.column_count() >= 1);
		UnaryExecutor::Execute<TA, TR, OP, SKIP_NULLS>(input.data[0], result, input.size());
	};

	template <class TA, class TB, class TR, class OP, bool IGNORE_NULL = false>
	static void BinaryFunction(DataChunk &input, ExpressionState &state, Vector &result) {
		assert(input.column_count() == 2);
		BinaryExecutor::ExecuteStandard<TA, TB, TR, OP, IGNORE_NULL>(input.data[0], input.data[1], result,
		                                                             input.size());
	};

public:
	template <class OP> static scalar_function_t GetScalarUnaryFunction(LogicalType type) {
		scalar_function_t function;
		switch (type.id()) {
		case LogicalTypeId::TINYINT:
			function = &ScalarFunction::UnaryFunction<int8_t, int8_t, OP>;
			break;
		case LogicalTypeId::SMALLINT:
			function = &ScalarFunction::UnaryFunction<int16_t, int16_t, OP>;
			break;
		case LogicalTypeId::INTEGER:
			function = &ScalarFunction::UnaryFunction<int32_t, int32_t, OP>;
			break;
		case LogicalTypeId::BIGINT:
			function = &ScalarFunction::UnaryFunction<int64_t, int64_t, OP>;
			break;
		case LogicalTypeId::HUGEINT:
			function = &ScalarFunction::UnaryFunction<hugeint_t, hugeint_t, OP>;
			break;
		case LogicalTypeId::FLOAT:
			function = &ScalarFunction::UnaryFunction<float, float, OP>;
			break;
		case LogicalTypeId::DOUBLE:
			function = &ScalarFunction::UnaryFunction<double, double, OP>;
			break;
		default:
			throw NotImplementedException("Unimplemented type for GetScalarUnaryFunction");
		}
		return function;
	}

	template <class TR, class OP> static scalar_function_t GetScalarUnaryFunctionFixedReturn(LogicalType type) {
		scalar_function_t function;
		switch (type.id()) {
		case LogicalTypeId::TINYINT:
			function = &ScalarFunction::UnaryFunction<int8_t, TR, OP>;
			break;
		case LogicalTypeId::SMALLINT:
			function = &ScalarFunction::UnaryFunction<int16_t, TR, OP>;
			break;
		case LogicalTypeId::INTEGER:
			function = &ScalarFunction::UnaryFunction<int32_t, TR, OP>;
			break;
		case LogicalTypeId::BIGINT:
			function = &ScalarFunction::UnaryFunction<int64_t, TR, OP>;
			break;
		case LogicalTypeId::HUGEINT:
			function = &ScalarFunction::UnaryFunction<hugeint_t, TR, OP>;
			break;
		case LogicalTypeId::FLOAT:
			function = &ScalarFunction::UnaryFunction<float, TR, OP>;
			break;
		case LogicalTypeId::DOUBLE:
			function = &ScalarFunction::UnaryFunction<double, TR, OP>;
			break;
		default:
			throw NotImplementedException("Unimplemented type for GetScalarUnaryFunctionFixedReturn");
		}
		return function;
	}
};

} // namespace duckdb
