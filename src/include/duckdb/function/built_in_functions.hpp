//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/function/built_in_functions.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include <string>

#include "duckdb/function/function.hpp"
#include "duckdb/catalog/catalog_transaction.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"

namespace duckdb {
class AggregateFunction;
class AggregateFunctionSet;
class Catalog;
class CopyFunction;
class PragmaFunction;
class PragmaFunctionSet;
class ScalarFunction;
class ScalarFunctionSet;
class TableFunction;
class TableFunctionSet;
class WindowFunction;
class WindowFunctionSet;

class BuiltinFunctions {
public:
	BuiltinFunctions(CatalogTransaction transaction, Catalog &catalog);
	~BuiltinFunctions();

	//! Initialize a catalog with all built-in functions
	void Initialize();

public:
	void AddFunction(AggregateFunctionSet set);
	void AddFunction(AggregateFunction function);
	void AddFunction(ScalarFunctionSet set);
	void AddFunction(PragmaFunction function);
	void AddFunction(const string &name, PragmaFunctionSet functions);
	void AddFunction(ScalarFunction function);
	void AddFunction(const vector<string> &names, ScalarFunction function);
	void AddFunction(TableFunctionSet set);
	void AddFunction(TableFunction function);
	void AddFunction(CopyFunction function);

	void AddCollation(string name, ScalarFunction function, bool combinable = false,
	                  bool not_required_for_equality = false);

private:
	CatalogTransaction transaction;
	Catalog &catalog;

private:
	template <class T>
	void Register() {
		T::RegisterFunction(*this);
	}

	// table-producing functions
	void RegisterTableScanFunctions();
	void RegisterSQLiteFunctions();
	void RegisterReadFunctions();
	void RegisterTableFunctions();
	void RegisterArrowFunctions();
	void RegisterSnifferFunction();
	void RegisterCopyFunctions();

	void RegisterExtensionOverloads();

	// pragmas
	void RegisterPragmaFunctions();

	void AddExtensionFunction(ScalarFunctionSet set);
};

} // namespace duckdb
