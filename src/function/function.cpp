#include "function/function.hpp"
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
	CreateAggregateFunctionInfo info;

	info.schema = DEFAULT_SCHEMA;
	info.name = T::GetName();

	info.state_size = T::GetStateSizeFunction();
	info.initialize = T::GetInitalizeFunction();
	info.update = T::GetUpdateFunction();
	info.finalize = T::GetFinalizeFunction();

	info.simple_initialize = T::GetSimpleInitializeFunction();
	info.simple_update = T::GetSimpleUpdateFunction();

	info.return_type = T::GetReturnTypeFunction();
	info.cast_arguments = T::GetCastArgumentsFunction();

	catalog.CreateFunction(transaction, &info);
}

template <class T> static void AddScalarFunction(Transaction &transaction, Catalog &catalog) {
	CreateScalarFunctionInfo info(T::GetFunction());

	// info.schema = DEFAULT_SCHEMA;
	// info.function = T::GetFunction();
	// info.name = info.function.name;

	catalog.CreateFunction(transaction, &info);
}

BuiltinFunctions::BuiltinFunctions(Transaction &transaction, Catalog &catalog) :
	transaction(transaction), catalog(catalog) {

}

void BuiltinFunctions::AddFunction(ScalarFunction function) {
	CreateScalarFunctionInfo info(function);
	catalog.CreateFunction(transaction, &info);
}

void BuiltinFunctions::Initialize() {
	AddTableFunction<PragmaTableInfo>(transaction, catalog);
	AddTableFunction<SQLiteMaster>(transaction, catalog);

	// distributive aggregates
	AddAggregateFunction<CountFunction>(transaction, catalog);
	AddAggregateFunction<CountStarFunction>(transaction, catalog);
	AddAggregateFunction<CovarPopFunction>(transaction, catalog);
	AddAggregateFunction<CovarSampFunction>(transaction, catalog);
	AddAggregateFunction<FirstFunction>(transaction, catalog);
	AddAggregateFunction<MaxFunction>(transaction, catalog);
	AddAggregateFunction<MinFunction>(transaction, catalog);
	AddAggregateFunction<StdDevPopFunction>(transaction, catalog);
	AddAggregateFunction<StdDevSampFunction>(transaction, catalog);
	AddAggregateFunction<StringAggFunction>(transaction, catalog);
	AddAggregateFunction<SumFunction>(transaction, catalog);
	AddAggregateFunction<VarPopFunction>(transaction, catalog);
	AddAggregateFunction<VarSampFunction>(transaction, catalog);

	// algebraic aggregates
	AddAggregateFunction<AvgFunction>(transaction, catalog);

	RegisterDateFunctions();
	RegisterMathFunctions();
	RegisterSequenceFunctions();
	RegisterSequenceFunctions();
	RegisterStringFunctions();
	RegisterTrigonometricsFunctions();
}
