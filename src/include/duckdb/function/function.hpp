//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/function/function.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/types/data_chunk.hpp"
#include "duckdb/common/unordered_set.hpp"
#include "duckdb/parser/column_definition.hpp"

namespace duckdb {
class CatalogEntry;
class Catalog;
class ClientContext;
class Expression;
class ExpressionExecutor;
class Transaction;

class AggregateFunction;
class AggregateFunctionSet;
class CopyFunction;
class ScalarFunctionSet;
class ScalarFunction;
class TableFunctionSet;
class TableFunction;

struct FunctionData {
	virtual ~FunctionData() {
	}

	virtual unique_ptr<FunctionData> Copy() {
		return make_unique<FunctionData>();
	};
};

struct TableFunctionData : public FunctionData {
	unique_ptr<FunctionData> Copy() override {
		throw NotImplementedException("Copy not required for table-producing function");
	}
	// used to pass on projections to table functions that support them. NB, can contain COLUMN_IDENTIFIER_ROW_ID
	vector<idx_t> column_ids;
};

//! Function is the base class used for any type of function (scalar, aggregate or simple function)
class Function {
public:
	Function(string name) : name(name) {
	}
	virtual ~Function() {
	}

	//! The name of the function
	string name;

public:
	//! Returns the formatted string name(arg1, arg2, ...)
	static string CallToString(string name, vector<LogicalType> arguments);
	//! Returns the formatted string name(arg1, arg2..) -> return_type
	static string CallToString(string name, vector<LogicalType> arguments, LogicalType return_type);

	//! Bind a scalar function from the set of functions and input arguments. Returns the index of the chosen function,
	//! or throws an exception if none could be found.
	static idx_t BindFunction(string name, vector<ScalarFunction> &functions, vector<LogicalType> &arguments);
	//! Bind an aggregate function from the set of functions and input arguments. Returns the index of the chosen
	//! function, or throws an exception if none could be found.
	static idx_t BindFunction(string name, vector<AggregateFunction> &functions, vector<LogicalType> &arguments);
	//! Bind a table function from the set of functions and input arguments. Returns the index of the chosen
	//! function, or throws an exception if none could be found.
	static idx_t BindFunction(string name, vector<TableFunction> &functions, vector<LogicalType> &arguments);
};

class SimpleFunction : public Function {
public:
	SimpleFunction(string name, vector<LogicalType> arguments, LogicalType varargs = LogicalType::INVALID)
	    : Function(name), arguments(move(arguments)), varargs(varargs) {
	}
	virtual ~SimpleFunction() {
	}

	//! The set of arguments of the function
	vector<LogicalType> arguments;
	//! The type of varargs to support, or LogicalTypeId::INVALID if the function does not accept variable length
	//! arguments
	LogicalType varargs;

public:
	virtual string ToString() {
		return Function::CallToString(name, arguments);
	}

	bool HasVarArgs() {
		return varargs.id() != LogicalTypeId::INVALID;
	}
};

class BaseScalarFunction : public SimpleFunction {
public:
	BaseScalarFunction(string name, vector<LogicalType> arguments, LogicalType return_type, bool has_side_effects,
	                   LogicalType varargs = LogicalType::INVALID)
	    : SimpleFunction(move(name), move(arguments), move(varargs)), return_type(return_type),
	      has_side_effects(has_side_effects) {
	}
	virtual ~BaseScalarFunction() {
	}

	//! Return type of the function
	LogicalType return_type;
	//! Whether or not the function has side effects (e.g. sequence increments, random() functions, NOW()). Functions
	//! with side-effects cannot be constant-folded.
	bool has_side_effects;

public:
	//! Cast a set of expressions to the arguments of this function
	void CastToFunctionArguments(vector<unique_ptr<Expression>> &children, vector<LogicalType> &types);

	string ToString() override {
		return Function::CallToString(name, arguments, return_type);
	}
};

class BuiltinFunctions {
public:
	BuiltinFunctions(ClientContext &transaction, Catalog &catalog);

	//! Initialize a catalog with all built-in functions
	void Initialize();

public:
	void AddFunction(AggregateFunctionSet set);
	void AddFunction(AggregateFunction function);
	void AddFunction(ScalarFunctionSet set);
	void AddFunction(ScalarFunction function);
	void AddFunction(vector<string> names, ScalarFunction function);
	void AddFunction(TableFunctionSet set);
	void AddFunction(TableFunction function);
	void AddFunction(CopyFunction function);

	void AddCollation(string name, ScalarFunction function, bool combinable = false,
	                  bool not_required_for_equality = false);

private:
	ClientContext &context;
	Catalog &catalog;

private:
	template <class T> void Register() {
		T::RegisterFunction(*this);
	}

	// table-producing functions
	void RegisterSQLiteFunctions();
	void RegisterReadFunctions();
	void RegisterTableFunctions();

	// aggregates
	void RegisterAlgebraicAggregates();
	void RegisterDistributiveAggregates();
	void RegisterNestedAggregates();

	// scalar functions
	void RegisterDateFunctions();
	void RegisterGenericFunctions();
	void RegisterMathFunctions();
	void RegisterOperators();
	void RegisterStringFunctions();
	void RegisterNestedFunctions();
	void RegisterSequenceFunctions();
	void RegisterTrigonometricsFunctions();
};

} // namespace duckdb
