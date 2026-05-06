#include "duckdb/function/function.hpp"

#include "duckdb/common/string_util.hpp"
#include "duckdb/common/types/hash.hpp"
#include "duckdb/function/built_in_functions.hpp"
#include "duckdb/function/scalar/string_functions.hpp"
#include "duckdb/function/scalar_function.hpp"

namespace duckdb {

bool FunctionProperties::operator==(const FunctionProperties &rhs) const {
	return stability == rhs.stability && null_handling == rhs.null_handling && errors == rhs.errors &&
	       collation_handling == rhs.collation_handling;
}

bool FunctionProperties::operator!=(const FunctionProperties &rhs) const {
	return !(*this == rhs);
}

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

bool FunctionData::SupportStatementCache() const {
	return true;
}

Function::Function(string name_p) : name(std::move(name_p)) {
}
Function::~Function() {
}

SimpleFunction::SimpleFunction(string name_p, vector<LogicalType> arguments_p, LogicalType return_type,
                               LogicalType varargs_p)
    : Function(std::move(name_p)), signature(std::move(arguments_p), std::move(varargs_p), std::move(return_type)) {
}

SimpleFunction::~SimpleFunction() {
}

static bool RequiresCatalogAndSchemaNamePrefix(const string &catalog_name, const string &schema_name) {
	return !catalog_name.empty() && catalog_name != SYSTEM_CATALOG && !schema_name.empty() &&
	       schema_name != DEFAULT_SCHEMA;
}

string FunctionParameter::ToString() const {
	return StringUtil::Format("%s %s", name, type.ToString());
}

string FunctionSignature::ToString() const {
	vector<string> params;
	params.reserve(parameters.size());
	for (auto &param : parameters) {
		params.push_back(param.ToString());
	}
	if (varargs.IsValid()) {
		params.push_back("[" + varargs.ToString() + "...]");
	}
	auto head = StringUtil::Format("(%s)", StringUtil::Join(params, ", "));
	if (return_type.IsValid()) {
		return head + " -> " + return_type.ToString();
	}
	return head;
}

string SimpleFunction::ToString() const {
	if (RequiresCatalogAndSchemaNamePrefix(catalog_name, schema_name)) {
		return StringUtil::Format("%s.%s.%s%s", catalog_name, schema_name, name, signature.ToString());
	}
	return name + signature.ToString();
}

SimpleNamedParameterFunction::SimpleNamedParameterFunction(string name_p, vector<LogicalType> arguments_p,
                                                           LogicalType varargs_p)
    : Function(std::move(name_p)), arguments(std::move(arguments_p)), varargs(std::move(varargs_p)) {
}

SimpleNamedParameterFunction::~SimpleNamedParameterFunction() {
}

string SimpleNamedParameterFunction::ToString() const {
	return Function::CallToString(catalog_name, schema_name, name, arguments, named_parameters);
}

bool SimpleNamedParameterFunction::HasNamedParameters() const {
	return !named_parameters.empty();
}

// add your initializer for new functions here
void BuiltinFunctions::Initialize() {
	RegisterTableScanFunctions();
	RegisterSQLiteFunctions();
	RegisterReadFunctions();
	RegisterTableFunctions();
	RegisterArrowFunctions();

	RegisterPragmaFunctions();

	RegisterCopyFunctions();

	// initialize collations
	AddCollation("nocase", LowerFun::GetFunction(), true);
	AddCollation("noaccent", StripAccentsFun::GetFunction(), true);
	AddCollation("nfc", NFCNormalizeFun::GetFunction());

	RegisterExtensionOverloads();
}

hash_t FunctionSignature::Hash() const {
	hash_t hash = return_type.Hash();
	for (auto &param : parameters) {
		hash = duckdb::CombineHash(hash, param.GetType().Hash());
	}
	return hash;
}

hash_t SimpleFunction::Hash() const {
	return signature.Hash();
}

string Function::CallToString(const string &catalog_name, const string &schema_name, const string &name,
                              const vector<LogicalType> &arguments, const LogicalType &varargs) {
	string result;
	if (RequiresCatalogAndSchemaNamePrefix(catalog_name, schema_name)) {
		result += catalog_name + "." + schema_name + ".";
	}
	result += name + "(";
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

string Function::CallToString(const string &catalog_name, const string &schema_name, const string &name,
                              const vector<LogicalType> &arguments, const LogicalType &varargs,
                              const LogicalType &return_type) {
	string result = CallToString(catalog_name, schema_name, name, arguments, varargs);
	result += " -> " + return_type.ToString();
	return result;
}

string Function::CallToString(const string &catalog_name, const string &schema_name, const string &name,
                              const vector<LogicalType> &arguments,
                              const named_parameter_type_map_t &named_parameters) {
	vector<string> input_arguments;
	input_arguments.reserve(arguments.size() + named_parameters.size());
	for (auto &arg : arguments) {
		input_arguments.push_back(arg.ToString());
	}
	for (auto &kv : named_parameters) {
		input_arguments.push_back(StringUtil::Format("%s : %s", kv.first, kv.second.ToString()));
	}
	string prefix = "";
	if (RequiresCatalogAndSchemaNamePrefix(catalog_name, schema_name)) {
		prefix = StringUtil::Format("%s.%s.", catalog_name, schema_name);
	}
	return StringUtil::Format("%s%s(%s)", prefix, name, StringUtil::Join(input_arguments, ", "));
}

void Function::EraseArgument(BoundSimpleFunction &bound_function, vector<unique_ptr<Expression>> &arguments,
                             idx_t argument_index) {
	if (bound_function.GetOriginalArguments().empty()) {
		bound_function.GetOriginalArguments() = bound_function.GetArguments();
	}
	D_ASSERT(arguments.size() == bound_function.GetArguments().size());
	D_ASSERT(argument_index < arguments.size());
	arguments.erase_at(argument_index);
	bound_function.GetArguments().erase_at(argument_index);
}

hash_t BoundSimpleFunction::Hash() const {
	hash_t hash = return_type.Hash();
	for (auto &arg : arguments) {
		hash = duckdb::CombineHash(hash, arg.Hash());
	}
	return hash;
}

string BoundSimpleFunction::ToString() const {
	return Function::CallToString(catalog_name, schema_name, name, arguments, LogicalTypeId::INVALID, return_type);
}

bool FunctionParameter::operator==(const FunctionParameter &other) const {
	return type == other.type && StringUtil::CIEquals(name, other.name);
}

bool FunctionParameter::operator!=(const FunctionParameter &other) const {
	return !(*this == other);
}

bool FunctionSignature::operator==(const FunctionSignature &other) const {
	return parameters == other.parameters && varargs == other.varargs && return_type == other.return_type;
}

bool FunctionSignature::operator!=(const FunctionSignature &other) const {
	return !(*this == other);
}

bool FunctionSignature::Equal(const FunctionSignature &other) const {
	if (parameters.size() != other.parameters.size()) {
		return false;
	}
	for (idx_t i = 0; i < parameters.size(); i++) {
		if (parameters[i].GetType() != other.parameters[i].GetType()) {
			return false;
		}
	}
	if (varargs != other.varargs) {
		return false;
	}
	if (return_type != other.return_type) {
		return false;
	}
	return true;
}

} // namespace duckdb
