#include "duckdb/function/function.hpp"

#include "duckdb/catalog/catalog.hpp"
#include "duckdb/catalog/catalog_entry/scalar_function_catalog_entry.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/common/types/hash.hpp"
#include "duckdb/function/scalar/string_functions.hpp"
#include "duckdb/function/scalar_function.hpp"
#include "duckdb/parser/parsed_data/create_aggregate_function_info.hpp"
#include "duckdb/parser/parsed_data/create_collation_info.hpp"
#include "duckdb/parser/parsed_data/create_copy_function_info.hpp"
#include "duckdb/parser/parsed_data/create_pragma_function_info.hpp"
#include "duckdb/parser/parsed_data/create_scalar_function_info.hpp"
#include "duckdb/parser/parsed_data/create_table_function_info.hpp"
#include "duckdb/parser/parsed_data/pragma_info.hpp"
#include "duckdb/planner/expression/bound_aggregate_expression.hpp"
#include "duckdb/planner/expression/bound_function_expression.hpp"

namespace duckdb {

FunctionData::~FunctionData() {
}

bool FunctionData::Equals(const FunctionData *left, const FunctionData *right) {
	if (left == right) {
		return true;
	}
	if (!left || !right) {
		return false;
	}
	return left->Equals(*right);
}

TableFunctionData::~TableFunctionData() {
}

unique_ptr<FunctionData> TableFunctionData::Copy() const {
	throw InternalException("Copy not supported for TableFunctionData");
}

bool TableFunctionData::Equals(const FunctionData &other) const {
	return false;
}

Function::Function(string name_p) : name(move(name_p)) {
}
Function::~Function() {
}

SimpleFunction::SimpleFunction(string name_p, vector<LogicalType> arguments_p, LogicalType varargs_p)
    : Function(move(name_p)), arguments(move(arguments_p)), varargs(move(varargs_p)) {
}

SimpleFunction::~SimpleFunction() {
}

string SimpleFunction::ToString() {
	return Function::CallToString(name, arguments);
}

bool SimpleFunction::HasVarArgs() const {
	return varargs.id() != LogicalTypeId::INVALID;
}

SimpleNamedParameterFunction::SimpleNamedParameterFunction(string name_p, vector<LogicalType> arguments_p,
                                                           LogicalType varargs_p)
    : SimpleFunction(move(name_p), move(arguments_p), move(varargs_p)) {
}

SimpleNamedParameterFunction::~SimpleNamedParameterFunction() {
}

string SimpleNamedParameterFunction::ToString() {
	return Function::CallToString(name, arguments, named_parameters);
}

bool SimpleNamedParameterFunction::HasNamedParameters() {
	return !named_parameters.empty();
}

BaseScalarFunction::BaseScalarFunction(string name_p, vector<LogicalType> arguments_p, LogicalType return_type_p,
                                       FunctionSideEffects side_effects, LogicalType varargs_p,
                                       FunctionNullHandling null_handling)
    : SimpleFunction(move(name_p), move(arguments_p), move(varargs_p)), return_type(move(return_type_p)),
      side_effects(side_effects), null_handling(null_handling) {
}

BaseScalarFunction::~BaseScalarFunction() {
}

string BaseScalarFunction::ToString() {
	return Function::CallToString(name, arguments, return_type);
}

// add your initializer for new functions here
void BuiltinFunctions::Initialize() {
	RegisterTableScanFunctions();
	RegisterSQLiteFunctions();
	RegisterReadFunctions();
	RegisterTableFunctions();
	RegisterArrowFunctions();

	RegisterAlgebraicAggregates();
	RegisterDistributiveAggregates();
	RegisterNestedAggregates();
	RegisterHolisticAggregates();
	RegisterRegressiveAggregates();

	RegisterDateFunctions();
	RegisterEnumFunctions();
	RegisterGenericFunctions();
	RegisterMathFunctions();
	RegisterOperators();
	RegisterSequenceFunctions();
	RegisterStringFunctions();
	RegisterNestedFunctions();
	RegisterTrigonometricsFunctions();

	RegisterPragmaFunctions();

	// initialize collations
	AddCollation("nocase", LowerFun::GetFunction(), true);
	AddCollation("noaccent", StripAccentsFun::GetFunction());
	AddCollation("nfc", NFCNormalizeFun::GetFunction());
}

BuiltinFunctions::BuiltinFunctions(ClientContext &context, Catalog &catalog) : context(context), catalog(catalog) {
}

void BuiltinFunctions::AddCollation(string name, ScalarFunction function, bool combinable,
                                    bool not_required_for_equality) {
	CreateCollationInfo info(move(name), move(function), combinable, not_required_for_equality);
	catalog.CreateCollation(context, &info);
}

void BuiltinFunctions::AddFunction(AggregateFunctionSet set) {
	CreateAggregateFunctionInfo info(move(set));
	catalog.CreateFunction(context, &info);
}

void BuiltinFunctions::AddFunction(AggregateFunction function) {
	CreateAggregateFunctionInfo info(move(function));
	catalog.CreateFunction(context, &info);
}

void BuiltinFunctions::AddFunction(PragmaFunction function) {
	CreatePragmaFunctionInfo info(move(function));
	catalog.CreatePragmaFunction(context, &info);
}

void BuiltinFunctions::AddFunction(const string &name, PragmaFunctionSet functions) {
	CreatePragmaFunctionInfo info(name, move(functions));
	catalog.CreatePragmaFunction(context, &info);
}

void BuiltinFunctions::AddFunction(ScalarFunction function) {
	CreateScalarFunctionInfo info(move(function));
	catalog.CreateFunction(context, &info);
}

void BuiltinFunctions::AddFunction(const vector<string> &names, ScalarFunction function) { // NOLINT: false positive
	for (auto &name : names) {
		function.name = name;
		AddFunction(function);
	}
}

void BuiltinFunctions::AddFunction(ScalarFunctionSet set) {
	CreateScalarFunctionInfo info(move(set));
	catalog.CreateFunction(context, &info);
}

void BuiltinFunctions::AddFunction(TableFunction function) {
	CreateTableFunctionInfo info(move(function));
	catalog.CreateTableFunction(context, &info);
}

void BuiltinFunctions::AddFunction(TableFunctionSet set) {
	CreateTableFunctionInfo info(move(set));
	catalog.CreateTableFunction(context, &info);
}

void BuiltinFunctions::AddFunction(CopyFunction function) {
	CreateCopyFunctionInfo info(move(function));
	catalog.CreateCopyFunction(context, &info);
}

hash_t BaseScalarFunction::Hash() const {
	hash_t hash = return_type.Hash();
	for (auto &arg : arguments) {
		duckdb::CombineHash(hash, arg.Hash());
	}
	return hash;
}

string Function::CallToString(const string &name, const vector<LogicalType> &arguments) {
	string result = name + "(";
	result += StringUtil::Join(arguments, arguments.size(), ", ",
	                           [](const LogicalType &argument) { return argument.ToString(); });
	return result + ")";
}

string Function::CallToString(const string &name, const vector<LogicalType> &arguments,
                              const LogicalType &return_type) {
	string result = CallToString(name, arguments);
	result += " -> " + return_type.ToString();
	return result;
}

string Function::CallToString(const string &name, const vector<LogicalType> &arguments,
                              const named_parameter_type_map_t &named_parameters) {
	vector<string> input_arguments;
	input_arguments.reserve(arguments.size() + named_parameters.size());
	for (auto &arg : arguments) {
		input_arguments.push_back(arg.ToString());
	}
	for (auto &kv : named_parameters) {
		input_arguments.push_back(StringUtil::Format("%s : %s", kv.first, kv.second.ToString()));
	}
	return StringUtil::Format("%s(%s)", name, StringUtil::Join(input_arguments, ", "));
}

void Function::EraseArgument(SimpleFunction &bound_function, vector<unique_ptr<Expression>> &arguments,
                             idx_t argument_index) {
	if (bound_function.original_arguments.empty()) {
		bound_function.original_arguments = bound_function.arguments;
	}
	D_ASSERT(arguments.size() == bound_function.arguments.size());
	D_ASSERT(argument_index < arguments.size());
	arguments.erase(arguments.begin() + argument_index);
	bound_function.arguments.erase(bound_function.arguments.begin() + argument_index);
}

} // namespace duckdb
