#include "duckdb/function/function.hpp"

#include "duckdb/common/string_util.hpp"
#include "duckdb/common/types/hash.hpp"
#include "duckdb/function/built_in_functions.hpp"
#include "duckdb/function/scalar/string_functions.hpp"
#include "duckdb/function/scalar_function.hpp"
#include "duckdb/parser/parsed_data/pragma_info.hpp"
#include "duckdb/planner/expression/bound_aggregate_expression.hpp"
#include "duckdb/planner/expression/bound_function_expression.hpp"
#include "duckdb/main/extension_entries.hpp"

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

Function::Function(string name_p) : name(std::move(name_p)) {
}
Function::~Function() {
}

SimpleFunction::SimpleFunction(string name_p, vector<LogicalType> arguments_p, LogicalType varargs_p)
    : Function(std::move(name_p)), arguments(std::move(arguments_p)), varargs(std::move(varargs_p)) {
}

SimpleFunction::~SimpleFunction() {
}

string SimpleFunction::ToString() const {
	return Function::CallToString(name, arguments, varargs);
}

bool SimpleFunction::HasVarArgs() const {
	return varargs.id() != LogicalTypeId::INVALID;
}

SimpleNamedParameterFunction::SimpleNamedParameterFunction(string name_p, vector<LogicalType> arguments_p,
                                                           LogicalType varargs_p)
    : SimpleFunction(std::move(name_p), std::move(arguments_p), std::move(varargs_p)) {
}

SimpleNamedParameterFunction::~SimpleNamedParameterFunction() {
}

string SimpleNamedParameterFunction::ToString() const {
	return Function::CallToString(name, arguments, named_parameters);
}

bool SimpleNamedParameterFunction::HasNamedParameters() const {
	return !named_parameters.empty();
}

BaseScalarFunction::BaseScalarFunction(string name_p, vector<LogicalType> arguments_p, LogicalType return_type_p,
                                       FunctionStability stability, LogicalType varargs_p,
                                       FunctionNullHandling null_handling)
    : SimpleFunction(std::move(name_p), std::move(arguments_p), std::move(varargs_p)),
      return_type(std::move(return_type_p)), stability(stability), null_handling(null_handling),
      collation_handling(FunctionCollationHandling::PROPAGATE_COLLATIONS) {
}

BaseScalarFunction::~BaseScalarFunction() {
}

string BaseScalarFunction::ToString() const {
	return Function::CallToString(name, arguments, varargs, return_type);
}

// add your initializer for new functions here
void BuiltinFunctions::Initialize() {
	RegisterTableScanFunctions();
	RegisterSQLiteFunctions();
	RegisterReadFunctions();
	RegisterTableFunctions();
	RegisterArrowFunctions();

	RegisterPragmaFunctions();

	// initialize collations
	AddCollation("nocase", LowerFun::GetFunction(), true);
	AddCollation("noaccent", StripAccentsFun::GetFunction(), true);
	AddCollation("nfc", NFCNormalizeFun::GetFunction());

	RegisterExtensionOverloads();
}

hash_t BaseScalarFunction::Hash() const {
	hash_t hash = return_type.Hash();
	for (auto &arg : arguments) {
		hash = duckdb::CombineHash(hash, arg.Hash());
	}
	return hash;
}

string Function::CallToString(const string &name, const vector<LogicalType> &arguments, const LogicalType &varargs) {
	string result = name + "(";
	vector<string> string_arguments;
	for (auto &arg : arguments) {
		string_arguments.push_back(arg.ToString());
	}
	if (varargs.IsValid()) {
		string_arguments.push_back("[" + varargs.ToString() + "...]");
	}
	result += StringUtil::Join(string_arguments, ", ");
	return result + ")";
}

string Function::CallToString(const string &name, const vector<LogicalType> &arguments, const LogicalType &varargs,
                              const LogicalType &return_type) {
	string result = CallToString(name, arguments, varargs);
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
	arguments.erase_at(argument_index);
	bound_function.arguments.erase_at(argument_index);
}

} // namespace duckdb
