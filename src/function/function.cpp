#include "function/function.hpp"

#include "catalog/catalog.hpp"
#include "function/scalar_function/list.hpp"
#include "function/table_function/list.hpp"
#include "parser/parsed_data.hpp"

using namespace duckdb;
using namespace std;

template <class T> static void AddTableFunction(Transaction &transaction, Catalog &catalog) {
	CreateTableFunctionInformation info;

	info.schema = DEFAULT_SCHEMA;
	info.name = T::GetName();
	T::GetArguments(info.arguments);
	T::GetReturnValues(info.return_values);
	info.init = T::GetInitFunction();
	info.function = T::GetFunction();
	info.final = T::GetFinalFunction();

	catalog.CreateTableFunction(transaction, &info);
}

template <class T> static void AddScalarFunction(Transaction &transaction, Catalog &catalog) {
	CreateScalarFunctionInformation info;

	info.schema = DEFAULT_SCHEMA;
	info.name = T::GetName();
	info.function = T::GetFunction();
	info.matches = T::GetMatchesArgumentFunction();
	info.return_type = T::GetReturnTypeFunction();
	info.bind = T::GetBindFunction();
	info.has_side_effects = T::HasSideEffects();

	catalog.CreateScalarFunction(transaction, &info);
}

void BuiltinFunctions::Initialize(Transaction &transaction, Catalog &catalog) {
	AddTableFunction<function::PragmaTableInfo>(transaction, catalog);
	AddTableFunction<function::SQLiteMaster>(transaction, catalog);

	AddScalarFunction<function::AbsFunction>(transaction, catalog);
	AddScalarFunction<function::ConcatFunction>(transaction, catalog);
	AddScalarFunction<function::DatePartFunction>(transaction, catalog);
	AddScalarFunction<function::LengthFunction>(transaction, catalog);
	AddScalarFunction<function::RoundFunction>(transaction, catalog);
	AddScalarFunction<function::SubstringFunction>(transaction, catalog);
	AddScalarFunction<function::YearFunction>(transaction, catalog);
	AddScalarFunction<function::UpperFunction>(transaction, catalog);
	AddScalarFunction<function::LowerFunction>(transaction, catalog);
	AddScalarFunction<function::NextvalFunction>(transaction, catalog);
}
