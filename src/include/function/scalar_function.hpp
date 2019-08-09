//===----------------------------------------------------------------------===//
//                         DuckDB
//
// function/scalar_function.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "function/function.hpp"

namespace duckdb {
class BoundFunctionExpression;

//! The type used for scalar functions
typedef void (*scalar_function_t)(ExpressionExecutor &exec, Vector inputs[], index_t input_count,
								BoundFunctionExpression &expr, Vector &result);
//! Binds the scalar function and creates the function data
typedef unique_ptr<FunctionData> (*bind_scalar_function_t)(BoundFunctionExpression &expr, ClientContext &context);
//! Adds the dependencies of this BoundFunctionExpression to the set of dependencies
typedef void (*dependency_function_t)(BoundFunctionExpression &expr, unordered_set<CatalogEntry *> &dependencies);

class ScalarFunction : public SimpleFunction {
public:
	ScalarFunction(string name, matches_argument_function_t matches, get_return_type_function_t return_type, scalar_function_t function, bool has_side_effects = false, bind_scalar_function_t bind = nullptr, dependency_function_t dependency = nullptr) :
		SimpleFunction(name, matches, return_type, has_side_effects), function(function), bind(bind), dependency(dependency) {}

	//! The main scalar function to execute
	scalar_function_t function;
	 //! The bind function (if any)
	bind_scalar_function_t bind;
	// The dependency function (if any)
	dependency_function_t dependency;
};

} // namespace duckdb
