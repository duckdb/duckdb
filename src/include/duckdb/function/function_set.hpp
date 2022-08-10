//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/function/function_set.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/function/aggregate_function.hpp"
#include "duckdb/function/scalar_function.hpp"
#include "duckdb/function/table_function.hpp"
#include "duckdb/function/pragma_function.hpp"

namespace duckdb {

template <class T>
class FunctionSet {
public:
	explicit FunctionSet(string name) : name(name) {
	}

	//! The name of the function set
	string name;
	//! The set of functions.
	vector<T> functions;

public:
	void AddFunction(T function) {
		functions.push_back(move(function));
	}
	idx_t Size() {
		return functions.size();
	}
	T GetFunctionByOffset(idx_t offset) {
		return functions[offset];
	}
};

class ScalarFunctionSet : public FunctionSet<ScalarFunction> {
public:
	explicit ScalarFunctionSet(string name) : FunctionSet(move(name)) {
	}

	ScalarFunction GetFunctionByArguments(const vector<LogicalType> &arguments) {
		string error;
		idx_t index = Function::BindFunction(name, *this, arguments, error);
		if (index == DConstants::INVALID_INDEX) {
			throw InternalException("Failed to find function %s(%s)\n%s", name, StringUtil::ToString(arguments, ","),
			                        error);
		}
		return GetFunctionByOffset(index);
	}
};

class AggregateFunctionSet : public FunctionSet<AggregateFunction> {
public:
	explicit AggregateFunctionSet(string name) : FunctionSet(move(name)) {
	}

	AggregateFunction GetFunctionByArguments(const vector<LogicalType> &arguments) {
		string error;
		idx_t index = Function::BindFunction(name, *this, arguments, error);
		if (index == DConstants::INVALID_INDEX) {
			throw InternalException("Failed to find function %s(%s)\n%s", name, StringUtil::ToString(arguments, ","),
			                        error);
		}
		return GetFunctionByOffset(index);
	}
};

class TableFunctionSet : public FunctionSet<TableFunction> {
public:
	explicit TableFunctionSet(string name) : FunctionSet(move(name)) {
	}

	TableFunction GetFunctionByArguments(const vector<LogicalType> &arguments) {
		string error;
		idx_t index = Function::BindFunction(name, *this, arguments, error);
		if (index == DConstants::INVALID_INDEX) {
			throw InternalException("Failed to find function %s(%s)\n%s", name, StringUtil::ToString(arguments, ","),
			                        error);
		}
		return GetFunctionByOffset(index);
	}
};

class PragmaFunctionSet : public FunctionSet<PragmaFunction> {
public:
	explicit PragmaFunctionSet(string name) : FunctionSet(move(name)) {
	}
};

} // namespace duckdb
