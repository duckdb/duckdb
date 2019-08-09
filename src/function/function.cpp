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

void BuiltinFunctions::AddFunction(TableFunction function) {
	CreateTableFunctionInfo info(function);
	catalog.CreateTableFunction(transaction, &info);
}

void BuiltinFunctions::Initialize() {
	RegisterSQLiteFunctions();

	RegisterAlgebraicAggregates();
	RegisterDistributiveAggregates();

	RegisterDateFunctions();
	RegisterMathFunctions();
	RegisterSequenceFunctions();
	RegisterStringFunctions();
	RegisterTrigonometricsFunctions();
}
