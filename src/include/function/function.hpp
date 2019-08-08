//===----------------------------------------------------------------------===//
//                         DuckDB
//
// function/function.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "common/types/data_chunk.hpp"
#include "common/unordered_set.hpp"
#include "parser/column_definition.hpp"

namespace duckdb {
class BoundFunctionExpression;
class CatalogEntry;
class Catalog;
class ClientContext;
class ExpressionExecutor;
class Transaction;

//! Type used for checking if a function matches the input arguments
typedef bool (*matches_argument_function_t)(vector<SQLType> &arguments);
//! Gets the return type of the function given the types of the input argument
typedef SQLType (*get_return_type_function_t)(vector<SQLType> &arguments);

//! Function is the base class used for any type of function (scalar, aggregate or simple function)
class Function {
public:
	Function(string name) : name(name) { }
	virtual ~Function() {}

	//! The name of the function
	string name;
};

class SimpleFunction : public Function {
public:
	SimpleFunction(string name, matches_argument_function_t matches, get_return_type_function_t return_type, bool has_side_effects) :
		Function(name), matches(matches), return_type(return_type), has_side_effects(has_side_effects) { }
	virtual ~SimpleFunction() {}

	//! Function that checks whether or not a set of arguments matches
	matches_argument_function_t matches;
	//! Function that gives the return type of the function given the input
	//! arguments
	get_return_type_function_t return_type;
	//! Whether or not the function has side effects (e.g. sequence increments, random() functions, NOW()). Functions
	//! with side-effects cannot be constant-folded.
	bool has_side_effects;
};

struct FunctionData {
	virtual ~FunctionData() {
	}

	virtual unique_ptr<FunctionData> Copy() = 0;
};

//! Type used for initialization function
typedef FunctionData *(*table_function_init_t)(ClientContext &);
//! Type used for table-returning function
typedef void (*table_function_t)(ClientContext &, DataChunk &input, DataChunk &output, FunctionData *dataptr);
//! Type used for final (cleanup) function
typedef void (*table_function_final_t)(ClientContext &, FunctionData *dataptr);


//! The type used for sizing hashed aggregate function states
typedef index_t (*aggregate_size_t)(TypeId return_type);
//! The type used for initializing hashed aggregate function states
typedef void (*aggregate_initialize_t)(data_ptr_t payload, TypeId return_type);
//! The type used for updating hashed aggregate functions
typedef void (*aggregate_update_t)(Vector inputs[], index_t input_count, Vector &result);
//! The type used for finalizing hashed aggregate function payloads
typedef void (*aggregate_finalize_t)(Vector &payloads, Vector &result);

//! The type used for initializing simple aggregate function
typedef Value (*aggregate_simple_initialize_t)();
//! The type used for updating simple aggregate functions
typedef void (*aggregate_simple_update_t)(Vector inputs[], index_t input_count, Value &result);

class BuiltinFunctions {
public:
	//! Initialize a catalog with all built-in functions
	static void Initialize(Transaction &transaction, Catalog &catalog);
};

} // namespace duckdb
