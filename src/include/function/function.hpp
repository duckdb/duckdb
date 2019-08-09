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
class CatalogEntry;
class Catalog;
class ClientContext;
class ExpressionExecutor;
class Transaction;

class AggregateFunction;
class ScalarFunction;

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

class BuiltinFunctions {
public:
	BuiltinFunctions(Transaction &transaction, Catalog &catalog);

	//! Initialize a catalog with all built-in functions
	void Initialize();
private:
	Transaction &transaction;
	Catalog &catalog;
private:
	void AddFunction(AggregateFunction function);
	void AddFunction(ScalarFunction function);

	void RegisterAlgebraicAggregates();
	void RegisterDistributiveAggregates();

	void RegisterDateFunctions();
	void RegisterMathFunctions();
	void RegisterStringFunctions();
	void RegisterSequenceFunctions();
	void RegisterTrigonometricsFunctions();
};

} // namespace duckdb
