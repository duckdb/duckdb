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
#include "duckdb/common/map.hpp"

namespace duckdb {

template <class T>
class FunctionSet {
public:
	explicit FunctionSet(string name) : name(name) {
	}

	//! The name of the function set
	string name;

	//! The set of functions. Each function has
	map<idx_t, T> functions;

public:
	void AddFunction(T function) {
		auto new_key = functions.size();
#ifdef DEBUG
		new_key += 42; // artifically introduce a large gap in the key space to catch problems where the index should be
		               // used instead of an offset
#endif
		AddFunction(move(function), new_key);
	}
	T GetFunction(idx_t key) {
		if (key == DConstants::INVALID_INDEX) {
			throw InternalException("Invalid function key for %s", name);
		}
		if (functions.find(key) == functions.end()) {
			throw InternalException("Function key %llu not in use for %s", key, name);
		}
		return functions.at(key);
	}
	idx_t Size() {
		return functions.size();
	}
	T GetFunctionByOffset(idx_t offset) {
		idx_t current_offset = 0;
		for (auto &entry : functions) {
			if (offset == current_offset) {
				return entry.second;
			}
			current_offset++;
		}
		throw InternalException("Function index %llu too big, only have %llu functions for %s", offset,
		                        functions.size(), name);
	}

	void AddFunction(T function, idx_t key) {
		if (functions.find(key) != functions.end()) {
			throw InternalException("Function index %llu already in use in %s", key, name);
		}
		function.name = name;
		function.function_set_key = key;
		functions.insert(std::pair<idx_t, T>(key, move(function)));
	}
};

class ScalarFunctionSet : public FunctionSet<ScalarFunction> {
public:
	explicit ScalarFunctionSet(string name) : FunctionSet(move(name)) {
	}
};

class AggregateFunctionSet : public FunctionSet<AggregateFunction> {
public:
	explicit AggregateFunctionSet(string name) : FunctionSet(move(name)) {
	}
};

class TableFunctionSet : public FunctionSet<TableFunction> {
public:
	explicit TableFunctionSet(string name) : FunctionSet(move(name)) {
	}
};

class PragmaFunctionSet : public FunctionSet<PragmaFunction> {
public:
	explicit PragmaFunctionSet(string name) : FunctionSet(move(name)) {
	}
};

} // namespace duckdb
