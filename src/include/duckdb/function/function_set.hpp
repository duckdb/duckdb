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
		functions.push_back(std::move(function));
	}
	idx_t Size() {
		return functions.size();
	}
	T GetFunctionByOffset(idx_t offset) {
		D_ASSERT(offset < functions.size());
		return functions[offset];
	}
	T &GetFunctionReferenceByOffset(idx_t offset) {
		D_ASSERT(offset < functions.size());
		return functions[offset];
	}
	bool MergeFunctionSet(FunctionSet<T> new_functions) {
		D_ASSERT(!new_functions.functions.empty());
		bool need_rewrite_entry = false;
		for (auto &new_func : new_functions.functions) {
			bool can_add = true;
			for (auto &func : functions) {
				if (new_func.Equal(func)) {
					can_add = false;
					break;
				}
			}
			if (can_add) {
				functions.push_back(new_func);
				need_rewrite_entry = true;
			}
		}
		return need_rewrite_entry;
	}
};

class ScalarFunctionSet : public FunctionSet<ScalarFunction> {
public:
	DUCKDB_API explicit ScalarFunctionSet();
	DUCKDB_API explicit ScalarFunctionSet(string name);
	DUCKDB_API explicit ScalarFunctionSet(ScalarFunction fun);

	DUCKDB_API ScalarFunction GetFunctionByArguments(ClientContext &context, const vector<LogicalType> &arguments);
};

class AggregateFunctionSet : public FunctionSet<AggregateFunction> {
public:
	DUCKDB_API explicit AggregateFunctionSet();
	DUCKDB_API explicit AggregateFunctionSet(string name);
	DUCKDB_API explicit AggregateFunctionSet(AggregateFunction fun);

	DUCKDB_API AggregateFunction GetFunctionByArguments(ClientContext &context, const vector<LogicalType> &arguments);
};

class TableFunctionSet : public FunctionSet<TableFunction> {
public:
	DUCKDB_API explicit TableFunctionSet(string name);
	DUCKDB_API explicit TableFunctionSet(TableFunction fun);

	TableFunction GetFunctionByArguments(ClientContext &context, const vector<LogicalType> &arguments);
};

class PragmaFunctionSet : public FunctionSet<PragmaFunction> {
public:
	DUCKDB_API explicit PragmaFunctionSet(string name);
	DUCKDB_API explicit PragmaFunctionSet(PragmaFunction fun);
};

} // namespace duckdb
