#include "duckdb/function/function.hpp"

#include "duckdb/common/string_util.hpp"
#include "duckdb/common/types/hash.hpp"
#include "duckdb/execution/expression_executor.hpp"
#include "duckdb/function/built_in_functions.hpp"
#include "duckdb/function/scalar/string_functions.hpp"
#include "duckdb/function/scalar_function.hpp"
#include "duckdb/planner/expression/bound_argument_pack.hpp"

namespace duckdb {

bool FunctionProperties::operator==(const FunctionProperties &rhs) const {
	return stability == rhs.stability && null_handling == rhs.null_handling && errors == rhs.errors &&
	       collation_handling == rhs.collation_handling && requires_ordered_execution == rhs.requires_ordered_execution;
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

Function::Function(Identifier name_p) : name(std::move(name_p)) {
}
Function::~Function() {
}

SimpleFunction::SimpleFunction(Identifier name_p, FunctionSignature signature_p)
    : Function(std::move(name_p)), signature(std::move(signature_p)) {
}

SimpleFunction::SimpleFunction(Identifier name_p, vector<LogicalType> arguments_p, LogicalType return_type)
    : Function(std::move(name_p)), signature(std::move(arguments_p), std::move(return_type)) {
}

SimpleFunction::~SimpleFunction() {
}

static bool RequiresCatalogAndSchemaNamePrefix(const Identifier &catalog_name, const Identifier &schema_name) {
	return !catalog_name.empty() && catalog_name != Identifier::SystemCatalog() && !schema_name.empty() &&
	       schema_name != Identifier::DefaultSchema();
}

string FunctionParameter::ToString() const {
	string prefix;
	switch (kind) {
	case FunctionParameterKind::VAR_POSITIONAL:
		prefix = "*";
		break;
	case FunctionParameterKind::VAR_KEYWORD:
		prefix = "**";
		break;
	default:
		break;
	}
	if (default_value) {
		return StringUtil::Format("%s%s %s := %s", prefix, name, type.ToString(), default_value->ToString());
	}
	return StringUtil::Format("%s%s %s", prefix, name, type.ToString());
}

string FunctionSignature::ToString() const {
	vector<string> params;
	params.reserve(parameters.size());
	bool emitted_keyword_only_marker = false;
	for (auto &param : parameters) {
		// keyword-only parameters that are not preceded by a "*args" parameter are introduced by a bare "*",
		// matching Python's notation
		if (param.GetKind() == FunctionParameterKind::KEYWORD_ONLY && !emitted_keyword_only_marker) {
			emitted_keyword_only_marker = true;
			if (!HasVarPositional()) {
				params.push_back("*");
			}
		}
		params.push_back(param.ToString());
	}
	auto head = StringUtil::Format("(%s)", StringUtil::Join(params, ", "));
	if (return_type.IsValid()) {
		return head + " -> " + return_type.ToString();
	}
	return head;
}

string SimpleFunction::ToString() const {
	if (RequiresCatalogAndSchemaNamePrefix(GetCatalogName(), GetSchemaName())) {
		return StringUtil::Format("%s.%s.%s%s", GetCatalogName(), GetSchemaName(), name, signature.ToString());
	}
	return name + signature.ToString();
}

SimpleNamedParameterFunction::SimpleNamedParameterFunction(Identifier name_p, vector<LogicalType> arguments_p,
                                                           LogicalType varargs_p)
    : Function(std::move(name_p)), arguments(std::move(arguments_p)), varargs(std::move(varargs_p)) {
}

SimpleNamedParameterFunction::~SimpleNamedParameterFunction() {
}

string SimpleNamedParameterFunction::ToString() const {
	return Function::CallToString(GetCatalogName(), GetSchemaName(), name, arguments, named_parameters);
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
		hash = duckdb::CombineHash(hash, duckdb::Hash(static_cast<uint8_t>(param.GetKind())));
	}
	return hash;
}

hash_t SimpleFunction::Hash() const {
	return signature.Hash();
}

string Function::CallToString(const Identifier &catalog_name, const Identifier &schema_name, const Identifier &name,
                              const vector<LogicalType> &arguments,
                              const vector<pair<Identifier, LogicalType>> &named_arguments) {
	string result;
	if (RequiresCatalogAndSchemaNamePrefix(catalog_name, schema_name)) {
		result += catalog_name + "." + schema_name + ".";
	}
	result += name + "(";
	vector<string> string_arguments;
	for (auto &arg : arguments) {
		string_arguments.push_back(arg.ToString());
	}

	for (const auto &[arg_name, arg_type] : named_arguments) {
		string_arguments.push_back(StringUtil::Format("%s := %s", arg_name, arg_type.ToString()));
	}

	result += StringUtil::Join(string_arguments, ", ");
	return result + ")";
}

string Function::CallToString(const Identifier &catalog_name, const Identifier &schema_name, const Identifier &name,
                              const vector<LogicalType> &arguments, const LogicalType &return_type) {
	string result = CallToString(catalog_name, schema_name, name, arguments, vector<pair<Identifier, LogicalType>> {});
	result += " -> " + return_type.ToString();
	return result;
}

string Function::CallToString(const Identifier &catalog_name, const Identifier &schema_name, const Identifier &name,
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
	if (ArgumentPack::IsPackType(bound_function.GetArguments()[argument_index])) {
		throw InternalException("%s: cannot erase argument %llu - it holds a packed argument list",
		                        bound_function.GetName(), argument_index);
	}
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
	return Function::CallToString(catalog_name, schema_name, name, arguments, return_type);
}

bool FunctionParameter::operator==(const FunctionParameter &other) const {
	return type == other.type && name == other.name && kind == other.kind;
}

bool FunctionParameter::operator!=(const FunctionParameter &other) const {
	return !(*this == other);
}

bool FunctionSignature::operator==(const FunctionSignature &other) const {
	return parameters == other.parameters && return_type == other.return_type;
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
		if (parameters[i].GetKind() != other.parameters[i].GetKind()) {
			return false;
		}
	}
	if (return_type != other.return_type) {
		return false;
	}
	return true;
}

//----------------------------------------------------------------------------------------------------------------------
// Bind Function Input
//----------------------------------------------------------------------------------------------------------------------
vector<reference<Expression>> BindFunctionInput::GetUnpackedArguments() const {
	vector<reference<Expression>> result;
	for (auto &argument : arguments) {
		if (!ArgumentPack::IsPackType(argument->GetReturnType())) {
			result.push_back(*argument);
			continue;
		}
		for (auto &packed : ArgumentPack::GetPackedChildren(*argument)) {
			result.push_back(*packed);
		}
	}
	return result;
}

Value BindFunctionInput::GetConstant(idx_t arg_idx, bool accept_null) const {
	if (arg_idx >= arguments.size()) {
		throw InternalException("%s: Argument index %llu is out of range", function.GetName(), arg_idx);
	}
	const auto &expr = *arguments[arg_idx];
	// an unresolved parameter or an as-yet-unknown type (e.g. a macro/prepared argument) - defer binding
	if (expr.HasParameter() || expr.GetReturnType().id() == LogicalTypeId::UNKNOWN) {
		throw ParameterNotResolvedException();
	}
	// Use the argument name if available, otherwise use the argument index
	string argument_name;
	if (argument_names && arg_idx < argument_names->size() && !argument_names->at(arg_idx).empty()) {
		argument_name = StringUtil::Format("The '%s' argument", argument_names->at(arg_idx));
	} else {
		argument_name = StringUtil::Format("Argument #%llu", arg_idx + 1);
	}
	if (!expr.IsFoldable()) {
		throw BinderException(expr, "%s in function '%s' must be a constant expression", argument_name,
		                      function.GetName());
	}
	auto value = ExpressionExecutor::EvaluateScalar(context, expr);
	if (!accept_null && value.IsNull()) {
		throw BinderException(expr, "%s in function '%s' must not be NULL", argument_name, function.GetName());
	}
	return value;
}

Value BindFunctionInput::GetConstant(const Identifier &name, bool accept_null) const {
	const auto arg_idx = GetArgumentIndex(name);
	if (!arg_idx.IsValid()) {
		throw InternalException("Function '%s' does not have a parameter named '%s'", function.GetName(), name);
	}
	return GetConstant(arg_idx.GetIndex(), accept_null);
}

optional<Value> BindFunctionInput::TryGetConstant(idx_t arg_idx) const {
	if (arg_idx >= arguments.size()) {
		return {};
	}
	const auto &expr = *arguments[arg_idx];
	if (expr.HasParameter() || expr.GetReturnType().id() == LogicalTypeId::UNKNOWN) {
		return {};
	}
	if (!expr.IsFoldable()) {
		return {};
	}
	return ExpressionExecutor::EvaluateScalar(context, expr);
}

optional<Value> BindFunctionInput::TryGetConstant(const Identifier &name) const {
	const auto arg_idx = GetArgumentIndex(name);
	if (arg_idx.IsValid()) {
		return TryGetConstant(arg_idx.GetIndex());
	}
	return {};
}

optional_idx BindFunctionInput::GetArgumentIndex(const Identifier &name) const {
	if (!argument_names) {
		throw InternalException("Function '%s' was bound without argument names, cannot look up argument '%s' by name",
		                        function.GetName(), name);
	}
	// The binder resolves every argument to the slot of the parameter it fills, and reports that parameter's name.
	for (idx_t arg_idx = 0; arg_idx < argument_names->size(); arg_idx++) {
		if ((*argument_names)[arg_idx] == name) {
			return optional_idx(arg_idx);
		}
	}
	return optional_idx();
}

} // namespace duckdb
