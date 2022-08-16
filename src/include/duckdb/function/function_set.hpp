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
	T &GetFunctionReferenceByOffset(idx_t offset) {
		return functions[offset];
	}
	bool MergeFunctionSet(FunctionSet<T> new_functions) {
		D_ASSERT(!new_functions.functions.empty());
		bool need_rewrite_entry = false;
		idx_t size_new_func = new_functions.functions.size();
		for (idx_t exist_idx = 0; exist_idx < functions.size(); ++exist_idx) {
			bool can_add = true;
			for (idx_t new_idx = 0; new_idx < size_new_func; ++new_idx) {
				if (new_functions.functions[new_idx].Equal(functions[exist_idx])) {
					can_add = false;
					break;
				}
			}
			if (can_add) {
				new_functions.functions.push_back(functions[exist_idx]);
				need_rewrite_entry = true;
			}
		}
		return need_rewrite_entry;
	}
};

class ScalarFunctionSet : public FunctionSet<ScalarFunction> {
public:
	DUCKDB_API explicit ScalarFunctionSet(string name);

	DUCKDB_API ScalarFunction GetFunctionByArguments(const vector<LogicalType> &arguments);
};

class AggregateFunctionSet : public FunctionSet<AggregateFunction> {
public:
	DUCKDB_API explicit AggregateFunctionSet(string name);

	DUCKDB_API AggregateFunction GetFunctionByArguments(const vector<LogicalType> &arguments);
};

class TableFunctionSet : public FunctionSet<TableFunction> {
public:
	DUCKDB_API explicit TableFunctionSet(string name);

	TableFunction GetFunctionByArguments(const vector<LogicalType> &arguments);
};

class PragmaFunctionSet : public FunctionSet<PragmaFunction> {
public:
	explicit PragmaFunctionSet(string name) : FunctionSet(move(name)) {
	}
};

} // namespace duckdb
