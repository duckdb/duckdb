#include "function/function.hpp"

#include "catalog/catalog.hpp"
#include "function/scalar_function/list.hpp"
#include "function/table_function/list.hpp"
#include "parser/parsed_data/create_scalar_function_info.hpp"
#include "parser/parsed_data/create_table_function_info.hpp"

using namespace duckdb;
using namespace std;

template <class T> static void AddTableFunction(Transaction &transaction, Catalog &catalog) {
	CreateTableFunctionInfo info;

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
	CreateScalarFunctionInfo info;

	info.schema = DEFAULT_SCHEMA;
	info.name = T::GetName();
	info.function = T::GetFunction();
	info.matches = T::GetMatchesArgumentFunction();
	info.return_type = T::GetReturnTypeFunction();
	info.bind = T::GetBindFunction();
	info.dependency = T::GetDependencyFunction();
	info.has_side_effects = T::HasSideEffects();

	catalog.CreateScalarFunction(transaction, &info);
}

void BuiltinFunctions::Initialize(Transaction &transaction, Catalog &catalog) {
	AddTableFunction<PragmaTableInfo>(transaction, catalog);
	AddTableFunction<SQLiteMaster>(transaction, catalog);

	AddScalarFunction<AbsFunction>(transaction, catalog);
	AddScalarFunction<ConcatFunction>(transaction, catalog);
	AddScalarFunction<DatePartFunction>(transaction, catalog);
	AddScalarFunction<LengthFunction>(transaction, catalog);
	AddScalarFunction<RoundFunction>(transaction, catalog);
	AddScalarFunction<SubstringFunction>(transaction, catalog);
	AddScalarFunction<YearFunction>(transaction, catalog);
	AddScalarFunction<UpperFunction>(transaction, catalog);
	AddScalarFunction<LowerFunction>(transaction, catalog);
	AddScalarFunction<NextvalFunction>(transaction, catalog);
	AddScalarFunction<RegexpMatchesFunction>(transaction, catalog);
	AddScalarFunction<RegexpReplaceFunction>(transaction, catalog);
}
