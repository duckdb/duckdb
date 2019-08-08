#include "function/function.hpp"

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
	CreateScalarFunctionInfo info;

	info.schema = DEFAULT_SCHEMA;
	info.name = T::GetName();
	info.function = T::GetFunction();
	info.matches = T::GetMatchesArgumentFunction();
	info.return_type = T::GetReturnTypeFunction();
	info.bind = T::GetBindFunction();
	info.dependency = T::GetDependencyFunction();
	info.has_side_effects = T::HasSideEffects();

	catalog.CreateFunction(transaction, &info);
}

void BuiltinFunctions::Initialize(Transaction &transaction, Catalog &catalog) {
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
	AddAggregateFunction<SumFunction>(transaction, catalog);
	AddAggregateFunction<VarPopFunction>(transaction, catalog);
	AddAggregateFunction<VarSampFunction>(transaction, catalog);

	// algebraic aggregates
	AddAggregateFunction<AvgFunction>(transaction, catalog);

	// math
	AddScalarFunction<AbsFunction>(transaction, catalog);
	AddScalarFunction<CbRtFunction>(transaction, catalog);
	AddScalarFunction<DegreesFunction>(transaction, catalog);
	AddScalarFunction<RadiansFunction>(transaction, catalog);
	AddScalarFunction<ExpFunction>(transaction, catalog);
	AddScalarFunction<RoundFunction>(transaction, catalog);
	AddScalarFunction<CeilFunction>(transaction, catalog);
	AddScalarFunction<CeilingFunction>(transaction, catalog);
	AddScalarFunction<FloorFunction>(transaction, catalog);
	AddScalarFunction<PiFunction>(transaction, catalog);
	AddScalarFunction<RandomFunction>(transaction, catalog);
	AddScalarFunction<SqrtFunction>(transaction, catalog);
	AddScalarFunction<LnFunction>(transaction, catalog);
	AddScalarFunction<LogFunction>(transaction, catalog);
	AddScalarFunction<Log10Function>(transaction, catalog);
	AddScalarFunction<Log2Function>(transaction, catalog);
	AddScalarFunction<SignFunction>(transaction, catalog);
	AddScalarFunction<ModFunction>(transaction, catalog);
	AddScalarFunction<PowFunction>(transaction, catalog);
	AddScalarFunction<PowerFunction>(transaction, catalog);

	// Trignometric
	AddScalarFunction<SinFunction>(transaction, catalog);
	AddScalarFunction<CosFunction>(transaction, catalog);
	AddScalarFunction<TanFunction>(transaction, catalog);
	AddScalarFunction<ASinFunction>(transaction, catalog);
	AddScalarFunction<ACosFunction>(transaction, catalog);
	AddScalarFunction<ATanFunction>(transaction, catalog);
	AddScalarFunction<CoTFunction>(transaction, catalog);
	AddScalarFunction<ATan2Function>(transaction, catalog);

	// strings
	AddScalarFunction<ConcatFunction>(transaction, catalog);
	AddScalarFunction<LengthFunction>(transaction, catalog);
	AddScalarFunction<SubstringFunction>(transaction, catalog);
	AddScalarFunction<UpperFunction>(transaction, catalog);
	AddScalarFunction<LowerFunction>(transaction, catalog);

	// regex
	AddScalarFunction<RegexpMatchesFunction>(transaction, catalog);
	AddScalarFunction<RegexpReplaceFunction>(transaction, catalog);

	// datetime
	AddScalarFunction<DatePartFunction>(transaction, catalog);
	AddScalarFunction<YearFunction>(transaction, catalog);

	// timestamp
	AddScalarFunction<AgeFunction>(transaction, catalog);

	// misc
	AddScalarFunction<NextvalFunction>(transaction, catalog);
}
