#include "function/function.hpp"
#include "function/aggregate_function.hpp"
#include "function/scalar_function.hpp"

#include "catalog/catalog.hpp"
#include "function/aggregate_function/list.hpp"
#include "function/scalar_function/list.hpp"
#include "function/table_function/list.hpp"
#include "parser/parsed_data/create_aggregate_function_info.hpp"
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

template <class T> static void AddAggregateFunction(Transaction &transaction, Catalog &catalog) {
	CreateAggregateFunctionInfo info(AggregateFunction(T::GetName(), T::GetReturnTypeFunction(), T::GetStateSizeFunction(), T::GetInitalizeFunction(), T::GetUpdateFunction(), T::GetFinalizeFunction(), T::GetSimpleInitializeFunction(), T::GetSimpleUpdateFunction()));
	catalog.CreateFunction(transaction, &info);
}

BuiltinFunctions::BuiltinFunctions(Transaction &transaction, Catalog &catalog) :
	transaction(transaction), catalog(catalog) {

}

void BuiltinFunctions::AddFunction(AggregateFunction function) {
	CreateAggregateFunctionInfo info(function);
	catalog.CreateFunction(transaction, &info);
}

void BuiltinFunctions::AddFunction(ScalarFunction function) {
	CreateScalarFunctionInfo info(function);
	catalog.CreateFunction(transaction, &info);
}

void BuiltinFunctions::Initialize() {
	AddTableFunction<PragmaTableInfo>(transaction, catalog);
	AddTableFunction<SQLiteMaster>(transaction, catalog);

	RegisterAlgebraicAggregates();
	RegisterDistributiveAggregates();

	RegisterDateFunctions();
	RegisterMathFunctions();
	RegisterSequenceFunctions();
	RegisterStringFunctions();
	RegisterTrigonometricsFunctions();
}
