#include "function/function.hpp"
#include "function/aggregate_function.hpp"
#include "function/scalar_function.hpp"

#include "catalog/catalog.hpp"
// #include "function/aggregate/list.hpp"
// #include "function/scalar/list.hpp"
// #include "function/table/list.hpp"
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

void BuiltinFunctions::AddFunction(FunctionSet set) {
	CreateScalarFunctionInfo info(set);
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
	RegisterOperators();
	RegisterSequenceFunctions();
	RegisterStringFunctions();
	RegisterTrigonometricsFunctions();
}


string Function::CallToString(string name, vector<SQLType> arguments) {
	string result = name + "(";
	for (index_t i = 0; i < arguments.size(); i++) {
		if (i != 0) {
			result += ", ";
		}
		result += SQLTypeToString(arguments[i]);
	}
	return result + ")";
}

string Function::CallToString(string name, vector<SQLType> arguments, SQLType return_type) {
	string result = CallToString(name, arguments);
	result += " -> " + SQLTypeToString(return_type);
	return result;
}
