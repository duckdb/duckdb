//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/function/scalar/struct_functions.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/function/scalar_function.hpp"
#include "duckdb/function/function_set.hpp"

namespace duckdb {


struct StructPackBindData : public FunctionData {
	SQLType stype;

	StructPackBindData(SQLType stype) : stype(stype) {
	}

	unique_ptr<FunctionData> Copy() override {
		return make_unique<StructPackBindData>(stype);
	}
};


struct StructPackFun {
	static void RegisterFunction(BuiltinFunctions &set);
};

struct StructExtractBindData : public FunctionData {
	string key;
	index_t index;
	TypeId type;

	StructExtractBindData(string key, index_t index, TypeId type) : key(key), index(index), type(type) {
	}

	unique_ptr<FunctionData> Copy() override {
		return make_unique<StructExtractBindData>(key, index, type);
	}
};


struct StructExtractFun {
	static void RegisterFunction(BuiltinFunctions &set);
};

} // namespace duckdb
