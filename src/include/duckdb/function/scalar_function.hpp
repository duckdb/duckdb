//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/function/scalar_function.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/function/function.hpp"
#include "duckdb/common/vector_operations/vector_operations.hpp"
#include "duckdb/common/vector_operations/binary_loops.hpp"
#include "duckdb/execution/expression_executor_state.hpp"

namespace duckdb {
class BoundFunctionExpression;

//! The type used for scalar functions
typedef void (*scalar_function_t)(DataChunk &input, ExpressionState &state, Vector &result);
//! Binds the scalar function and creates the function data
typedef unique_ptr<FunctionData> (*bind_scalar_function_t)(BoundFunctionExpression &expr, ClientContext &context);
//! Adds the dependencies of this BoundFunctionExpression to the set of dependencies
typedef void (*dependency_function_t)(BoundFunctionExpression &expr, unordered_set<CatalogEntry *> &dependencies);

class ScalarFunction : public SimpleFunction {
public:
	ScalarFunction(string name, vector<SQLType> arguments, SQLType return_type, scalar_function_t function,
	               bool has_side_effects = false, bind_scalar_function_t bind = nullptr,
	               dependency_function_t dependency = nullptr)
	    : SimpleFunction(name, arguments, return_type, has_side_effects), function(function), bind(bind),
	      dependency(dependency) {
	}

	ScalarFunction(vector<SQLType> arguments, SQLType return_type, scalar_function_t function,
	               bool has_side_effects = false, bind_scalar_function_t bind = nullptr,
	               dependency_function_t dependency = nullptr)
	    : ScalarFunction(string(), arguments, return_type, function, has_side_effects, bind, dependency) {
	}

	//! The main scalar function to execute
	scalar_function_t function;
	//! The bind function (if any)
	bind_scalar_function_t bind;
	// The dependency function (if any)
	dependency_function_t dependency;

	bool operator==(const ScalarFunction &rhs) const {
		return function == rhs.function && bind == rhs.bind && dependency == rhs.dependency;
	}
	bool operator!=(const ScalarFunction &rhs) const {
		return !(*this == rhs);
	}

public:
	static void NopFunction(DataChunk &input, ExpressionState &state, Vector &result) {
		assert(input.column_count >= 1);
		result.Reference(input.data[0]);
	};

	template <class TA, class TR, class OP>
	static void UnaryFunction(DataChunk &input, ExpressionState &state, Vector &result) {
		assert(input.column_count >= 1);
		VectorOperations::UnaryExec<TA, TR>(input.data[0], result, OP::template Operation<TR>);
	};

	template <class TA, class TB, class TR, class OP, bool IGNORE_NULL = false>
	static void BinaryFunction(DataChunk &input, ExpressionState &state, Vector &result)  {
		assert(input.column_count == 2);
		templated_binary_loop<TA, TB, TR, OP, IGNORE_NULL>(input.data[0], input.data[1], result);
	};
};

} // namespace duckdb
