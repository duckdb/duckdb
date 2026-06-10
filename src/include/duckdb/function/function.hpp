//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/function/function.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/identifier.hpp"
#include "duckdb/common/named_parameter_map.hpp"
#include "duckdb/common/types/data_chunk.hpp"
#include "duckdb/common/unordered_set.hpp"
#include "duckdb/main/external_dependencies.hpp"
#include "duckdb/parser/column_definition.hpp"
#include "duckdb/common/enums/function_errors.hpp"
#include "duckdb/common/optional_idx.hpp"
#include "fmt/core.h"

namespace duckdb {
class CatalogEntry;
class Catalog;
class ClientContext;
class Expression;
class ExpressionExecutor;
class Transaction;

class AggregateFunction;
class AggregateFunctionSet;
class CopyFunction;
class PragmaFunction;
class PragmaFunctionSet;
class ScalarFunctionSet;
class ScalarFunction;
class TableFunctionSet;
class TableFunction;
class SimpleFunction;
class WindowFunction;
class WindowFunctionSet;
class BoundSimpleFunction;

struct PragmaInfo;

//! The default null handling is NULL in, NULL out
enum class FunctionNullHandling : uint8_t { DEFAULT_NULL_HANDLING = 0, SPECIAL_HANDLING = 1 };
//! The stability of the function, used by the optimizer
//! CONSISTENT              -> this function always returns the same result when given the same input, no variance
//! CONSISTENT_WITHIN_QUERY -> this function returns the same result WITHIN the same query/transaction
//!                            but the result might change across queries (e.g. NOW(), CURRENT_TIME)
//! VOLATILE                -> the result of this function might change per row (e.g. RANDOM())
enum class FunctionStability : uint8_t { CONSISTENT = 0, VOLATILE = 1, CONSISTENT_WITHIN_QUERY = 2 };

//! How to handle collations
//! PROPAGATE_COLLATIONS        -> this function combines collation from its inputs and emits them again (default)
//! PUSH_COMBINABLE_COLLATIONS  -> combinable collations are executed for the input arguments
//! IGNORE_COLLATIONS           -> collations are completely ignored by the function
enum class FunctionCollationHandling : uint8_t {
	PROPAGATE_COLLATIONS = 0,
	PUSH_COMBINABLE_COLLATIONS = 1,
	IGNORE_COLLATIONS = 2
};

struct FunctionData {
	DUCKDB_API virtual ~FunctionData();

	DUCKDB_API virtual unique_ptr<FunctionData> Copy() const = 0;
	DUCKDB_API virtual bool Equals(const FunctionData &other) const = 0;
	DUCKDB_API static bool Equals(const FunctionData *left, const FunctionData *right);
	DUCKDB_API virtual bool SupportStatementCache() const;

	template <class TARGET>
	TARGET &Cast() {
		DynamicCastCheck<TARGET>(this);
		return reinterpret_cast<TARGET &>(*this);
	}
	template <class TARGET>
	const TARGET &Cast() const {
		DynamicCastCheck<TARGET>(this);
		return reinterpret_cast<const TARGET &>(*this);
	}
	// FIXME: this function should be removed in the future
	template <class TARGET>
	TARGET &CastNoConst() const {
		return const_cast<TARGET &>(Cast<TARGET>()); // NOLINT: FIXME
	}
};

struct TableFunctionData : public FunctionData {
	// used to pass on projections to table functions that support them. NB, can contain COLUMN_IDENTIFIER_ROW_ID
	vector<idx_t> column_ids;

	DUCKDB_API ~TableFunctionData() override;

	DUCKDB_API unique_ptr<FunctionData> Copy() const override;
	DUCKDB_API bool Equals(const FunctionData &other) const override;
};

struct FunctionParameters {
	vector<Value> values;
	named_parameter_map_t named_parameters;
};

class FunctionParameter {
public:
	FunctionParameter(Identifier name, LogicalType type)
	    : name(std::move(name)), type(std::move(type)), default_value(nullptr) {
	}

	FunctionParameter(Identifier name, LogicalType type, Value value)
	    : name(std::move(name)), type(std::move(type)), default_value(make_shared_ptr<Value>(std::move(value))) {
	}

	string ToString() const;

	bool operator==(const FunctionParameter &other) const;
	bool operator!=(const FunctionParameter &other) const;

	auto GetName() const -> const Identifier & {
		return name;
	}
	auto SetName(Identifier name_p) -> void {
		name = std::move(name_p);
	}

	auto GetType() const -> const LogicalType & {
		return type;
	}
	auto SetType(LogicalType type_p) -> void {
		type = std::move(type_p);
	}

	auto GetDefaultValue() const -> optional_ptr<Value> {
		return default_value.get();
	}
	auto SetDefaultValue(Value value) -> void {
		default_value = make_shared_ptr<Value>(std::move(value));
	}
	auto HasDefaultValue() const -> bool {
		return default_value != nullptr;
	}

private:
	Identifier name;
	LogicalType type;
	shared_ptr<Value> default_value;
};

class FunctionSignature {
public:
	FunctionSignature() = default;

	FunctionSignature(vector<LogicalType> arguments, LogicalType varargs, LogicalType return_type)
	    : varargs(std::move(varargs)), return_type(std::move(return_type)) {
		for (auto &arg : arguments) {
			AddParameter(std::move(arg));
		}
	}
	FunctionSignature(vector<LogicalType> arguments, LogicalType return_type)
	    : FunctionSignature(std::move(arguments), LogicalType(LogicalTypeId::INVALID), std::move(return_type)) {
	}

	string ToString() const;

	bool operator==(const FunctionSignature &other) const;
	bool operator!=(const FunctionSignature &other) const;

	bool Equal(const FunctionSignature &other) const;

public:
	auto GetParameter(idx_t index) const -> const FunctionParameter & {
		return parameters[index];
	}
	auto GetParameter(idx_t index) -> FunctionParameter & {
		return parameters[index];
	}
	auto GetParameters() const -> const vector<FunctionParameter> & {
		return parameters;
	}
	auto GetParameterCount() const -> idx_t {
		return parameters.size();
	}

	auto GetReturnType() const -> const LogicalType & {
		return return_type;
	}
	auto SetReturnType(LogicalType return_type_p) -> void {
		return_type = std::move(return_type_p);
	}

	auto HasVarArgs() const -> bool {
		return varargs.id() != LogicalTypeId::INVALID;
	}
	auto GetVarArgs() const -> const LogicalType & {
		return varargs;
	}
	auto SetVarArgs(LogicalType varargs_p) -> void {
		varargs = std::move(varargs_p);
	}

	auto AddParameter(Identifier name, LogicalType type, Value default_value) -> FunctionSignature & {
		parameters.emplace_back(std::move(name), std::move(type), std::move(default_value));
		return *this;
	}

	auto AddParameter(Identifier name, LogicalType type) -> FunctionSignature & {
		parameters.emplace_back(std::move(name), std::move(type));
		return *this;
	}

	auto AddParameter(LogicalType type) -> FunctionSignature & {
		auto name = StringUtil::Format("col%d", parameters.size());
		parameters.emplace_back(Identifier(name), std::move(type));
		return *this;
	}

	auto GetParameterIndexByName(const Identifier &name) const -> optional_idx {
		// Parameter names are matched case-insensitively, consistent with SQL identifier semantics.
		for (idx_t i = 0; i < parameters.size(); i++) {
			if (parameters[i].GetName() == name) {
				return i;
			}
		}
		return optional_idx();
	}

	auto GetRequiredParameterCount() const -> idx_t {
		idx_t result = 0;
		for (const auto &param : parameters) {
			if (!param.HasDefaultValue()) {
				result++;
			}
		}
		return result;
	}

	void Verify() const {
		// Check for duplicate parameter names
		identifier_set_t seen_names;
		for (const auto &param : parameters) {
			if (seen_names.find(param.GetName()) != seen_names.end()) {
				throw InvalidInputException("Duplicate parameter name: %s", param.GetName());
			}
			seen_names.insert(param.GetName());
		}

		// Also check for default values that are not at the end of the parameter list
		bool found_default_value = false;
		for (const auto &param : parameters) {
			if (param.HasDefaultValue()) {
				found_default_value = true;
			} else if (found_default_value) {
				throw InvalidInputException(
				    "Parameters with default values must be at the end of the parameter list. Parameter '%s' does not "
				    "have a default value but follows a parameter with a default value.",
				    param.GetName());
			}
		}
	}

	hash_t Hash() const;

private:
	vector<FunctionParameter> parameters;
	LogicalType varargs;
	LogicalType return_type;
};

//! Function is the base class used for any type of function (scalar, aggregate or simple function)
class Function {
public:
	DUCKDB_API explicit Function(Identifier name);
	DUCKDB_API virtual ~Function();

	//! The name of the function
	Identifier name;
	//! Additional Information to specify function from it's name
	string extra_info;

	// Optional catalog name of the function
	Identifier catalog_name;

	// Optional schema name of the function
	Identifier schema_name;

public:
	auto SetName(Identifier name_p) -> void {
		name = std::move(name_p);
	}
	auto SetSchemaName(Identifier schema_name_p) -> void {
		schema_name = std::move(schema_name_p);
	}
	auto SetCatalogName(Identifier catalog_name_p) -> void {
		catalog_name = std::move(catalog_name_p);
	}

	const Identifier &GetName() const {
		return name;
	}
	const Identifier &GetSchemaName() const {
		return schema_name;
	}
	const Identifier &GetCatalogName() const {
		return catalog_name;
	}

	//! Returns the formatted string name(arg1, arg2, ...)
	DUCKDB_API static string CallToString(const Identifier &catalog_name, const Identifier &schema_name,
	                                      const Identifier &name, const vector<LogicalType> &arguments,
	                                      const vector<pair<Identifier, LogicalType>> &named_arguments,
	                                      const LogicalType &varargs = LogicalType::INVALID);
	//! Returns the formatted string name(arg1, arg2..) -> return_type
	DUCKDB_API static string CallToString(const Identifier &catalog_name, const Identifier &schema_name,
	                                      const Identifier &name, const vector<LogicalType> &arguments,
	                                      const LogicalType &varargs, const LogicalType &return_type);
	//! Returns the formatted string name(arg1, arg2.., np1=a, np2=b, ...)
	DUCKDB_API static string CallToString(const Identifier &catalog_name, const Identifier &schema_name,
	                                      const Identifier &name, const vector<LogicalType> &arguments,
	                                      const named_parameter_type_map_t &named_parameters);
	//! Used in the bind to erase an argument from a function
	DUCKDB_API static void EraseArgument(BoundSimpleFunction &bound_function, vector<unique_ptr<Expression>> &arguments,
	                                     idx_t argument_index);
};

class SimpleFunction : public Function {
public:
	DUCKDB_API SimpleFunction(Identifier name, FunctionSignature signature);
	DUCKDB_API SimpleFunction(Identifier name, vector<LogicalType> arguments, LogicalType return_type,
	                          LogicalType varargs = LogicalType(LogicalTypeId::INVALID));
	DUCKDB_API ~SimpleFunction() override;

protected:
	FunctionSignature signature;

public:
	DUCKDB_API string ToString() const;
	DUCKDB_API hash_t Hash() const;

	FunctionSignature &GetSignature() {
		return signature;
	}
	const FunctionSignature &GetSignature() const {
		return signature;
	}

	const LogicalType &GetVarArgs() const {
		return signature.GetVarArgs();
	}

	void SetVarArgs(LogicalType varargs_p) {
		signature.SetVarArgs(std::move(varargs_p));
	}

	DUCKDB_API bool HasVarArgs() const {
		return signature.HasVarArgs();
	}

	void SetReturnType(LogicalType return_type_p) {
		signature.SetReturnType(std::move(return_type_p));
	}
	const LogicalType &GetReturnType() const {
		return signature.GetReturnType();
	}
};

class SimpleNamedParameterFunction : public Function {
public:
	DUCKDB_API SimpleNamedParameterFunction(Identifier name, vector<LogicalType> arguments,
	                                        LogicalType varargs = LogicalType(LogicalTypeId::INVALID));
	DUCKDB_API ~SimpleNamedParameterFunction() override;

	//! The set of arguments of the function
	vector<LogicalType> arguments;
	//! The set of original arguments of the function - only set if Function::EraseArgument is called
	//! Used for (de)serialization purposes
	vector<LogicalType> original_arguments;
	//! The type of varargs to support, or LogicalTypeId::INVALID if the function does not accept variable length
	//! arguments
	LogicalType varargs;

	//! The named parameters of the function
	named_parameter_type_map_t named_parameters;

public:
	DUCKDB_API virtual string ToString() const;
	DUCKDB_API bool HasNamedParameters() const;

	vector<LogicalType> &GetArguments() {
		return arguments;
	}
	const vector<LogicalType> &GetArguments() const {
		return arguments;
	}

	vector<LogicalType> &GetOriginalArguments() {
		return original_arguments;
	}
	const vector<LogicalType> &GetOriginalArguments() const {
		return original_arguments;
	}

	const LogicalType &GetVarArgs() const {
		return varargs;
	}
	LogicalType &GetVarArgs() {
		return varargs;
	}
	// TODO: Dont expose mutable accessor
	void SetVarArgs(LogicalType varargs_p) {
		varargs = std::move(varargs_p);
	}
	bool HasVarArgs() const {
		return varargs.id() != LogicalTypeId::INVALID;
	}
};

class FunctionProperties {
public:
	auto GetStability() const -> FunctionStability {
		return stability;
	}
	auto SetStability(FunctionStability value) -> void {
		stability = value;
	}

	auto GetNullHandling() const -> FunctionNullHandling {
		return null_handling;
	}
	auto SetNullHandling(FunctionNullHandling value) -> void {
		null_handling = value;
	}

	auto GetErrorMode() const -> FunctionErrors {
		return errors;
	}
	auto SetErrorMode(FunctionErrors value) -> void {
		errors = value;
	}

	auto GetCollationHandling() const -> FunctionCollationHandling {
		return collation_handling;
	}
	auto SetCollationHandling(FunctionCollationHandling value) -> void {
		collation_handling = value;
	}

	auto GetCaptureArgumentAliases() const -> bool {
		return capture_argument_aliases;
	}
	auto SetCaptureArgumentAliases(bool value) -> void {
		capture_argument_aliases = value;
	}

	// Helpers
	auto SetFallible() -> void {
		errors = FunctionErrors::CAN_THROW_RUNTIME_ERROR;
	}
	auto SetVolatile() -> void {
		stability = FunctionStability::VOLATILE;
	}

	bool operator==(const FunctionProperties &rhs) const;
	bool operator!=(const FunctionProperties &rhs) const;

public:
	FunctionStability stability = FunctionStability::CONSISTENT;
	//! How this function handles NULL values
	FunctionNullHandling null_handling = FunctionNullHandling::DEFAULT_NULL_HANDLING;
	//! Whether or not this function can throw an error
	FunctionErrors errors = FunctionErrors::CANNOT_ERROR;
	//! Collation handling of the function
	FunctionCollationHandling collation_handling = FunctionCollationHandling::PROPAGATE_COLLATIONS;
	//! Whether the binder should capture argument expression aliases as named-argument names when binding this
	//! function. This preserves the legacy behavior of functions such as struct_pack/row, which derived their
	//! (struct field) names from argument aliases and therefore allowed positional arguments after named ones.
	bool capture_argument_aliases = false;
};

class BoundSimpleFunction {
protected:
	Identifier name;
	Identifier schema_name;
	Identifier catalog_name;
	string extra_info;

	//! The set of arguments of the function
	vector<LogicalType> arguments;
	//! The set of original arguments of the function - only set if Function::EraseArgument is called
	//! Used for (de)serialization purposes
	vector<LogicalType> original_arguments;
	//! Return type of the function
	LogicalType return_type;

public:
	void SetName(Identifier name_p) {
		name = std::move(name_p);
	}

	const Identifier &GetName() const {
		return name;
	}
	const Identifier &GetSchemaName() const {
		return schema_name;
	}
	const Identifier &GetCatalogName() const {
		return catalog_name;
	}

	const string &GetExtraInfo() const {
		return extra_info;
	}

	DUCKDB_API string ToString() const;
	DUCKDB_API hash_t Hash() const;

	auto GetArguments() const -> const vector<LogicalType> & {
		return arguments;
	}
	auto GetArguments() -> vector<LogicalType> & {
		return arguments;
	}

	auto GetOriginalArguments() const -> const vector<LogicalType> & {
		return original_arguments;
	}
	auto GetOriginalArguments() -> vector<LogicalType> & {
		return original_arguments;
	}

	auto GetReturnType() const -> const LogicalType & {
		return return_type;
	}
	auto GetReturnType() -> LogicalType & {
		return return_type;
	}
	auto SetReturnType(LogicalType return_type_p) -> void {
		return_type = std::move(return_type_p);
	}
};

} // namespace duckdb
