//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/function/function.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/named_parameter_map.hpp"
#include "duckdb/common/types/data_chunk.hpp"
#include "duckdb/common/unordered_set.hpp"
#include "duckdb/main/external_dependencies.hpp"
#include "duckdb/parser/column_definition.hpp"
#include "duckdb/common/enums/function_errors.hpp"
#include "duckdb/common/optional_idx.hpp"

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

//! Function is the base class used for any type of function (scalar, aggregate or simple function)
class Function {
public:
	DUCKDB_API explicit Function(string name);
	DUCKDB_API virtual ~Function();

	//! The name of the function
	string name;
	//! Additional Information to specify function from it's name
	string extra_info;

	// Optional catalog name of the function
	string catalog_name;

	// Optional schema name of the function
	string schema_name;

public:
	//! Returns the formatted string name(arg1, arg2, ...)
	DUCKDB_API static string CallToString(const string &catalog_name, const string &schema_name, const string &name,
	                                      const vector<LogicalType> &arguments,
	                                      const LogicalType &varargs = LogicalType::INVALID);
	//! Returns the formatted string name(arg1, arg2..) -> return_type
	DUCKDB_API static string CallToString(const string &catalog_name, const string &schema_name, const string &name,
	                                      const vector<LogicalType> &arguments, const LogicalType &varargs,
	                                      const LogicalType &return_type);
	//! Returns the formatted string name(arg1, arg2.., np1=a, np2=b, ...)
	DUCKDB_API static string CallToString(const string &catalog_name, const string &schema_name, const string &name,
	                                      const vector<LogicalType> &arguments,
	                                      const named_parameter_type_map_t &named_parameters);

	//! Used in the bind to erase an argument from a function
	DUCKDB_API static void EraseArgument(SimpleFunction &bound_function, vector<unique_ptr<Expression>> &arguments,
	                                     idx_t argument_index);
	//! Used in the bind to erase an argument from a function
	DUCKDB_API static void EraseArgument(BoundSimpleFunction &bound_function, vector<unique_ptr<Expression>> &arguments,
	                                     idx_t argument_index);
};

class SimpleFunction : public Function {
public:
	DUCKDB_API SimpleFunction(string name, vector<LogicalType> arguments, LogicalType return_type,
	                          LogicalType varargs = LogicalType(LogicalTypeId::INVALID));
	DUCKDB_API ~SimpleFunction() override;

protected:
	//! The set of arguments of the function
	vector<LogicalType> arguments;
	//! The set of original arguments of the function - only set if Function::EraseArgument is called
	//! Used for (de)serialization purposes
	vector<LogicalType> original_arguments;
	//! The type of varargs to support, or LogicalTypeId::INVALID if the function does not accept variable length
	//! arguments
	LogicalType varargs;
	//! Return type of the function
	LogicalType return_type;

public:
	DUCKDB_API string ToString() const;
	DUCKDB_API hash_t Hash() const;

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
	} // TODO: Dont expose mutable accessor
	void SetVarArgs(LogicalType varargs_p) {
		varargs = std::move(varargs_p);
	}

	DUCKDB_API bool HasVarArgs() const;

	void SetReturnType(LogicalType return_type_p) {
		return_type = std::move(return_type_p);
	}
	const LogicalType &GetReturnType() const {
		return return_type;
	}
	LogicalType &GetReturnType() {
		return return_type;
	}
};

class SimpleNamedParameterFunction : public Function {
public:
	DUCKDB_API SimpleNamedParameterFunction(string name, vector<LogicalType> arguments,
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
};

class BoundSimpleFunction {
public:
	string name;
	string schema_name;
	string catalog_name;

protected:
	//! The set of arguments of the function
	vector<LogicalType> arguments;
	//! The set of original arguments of the function - only set if Function::EraseArgument is called
	//! Used for (de)serialization purposes
	vector<LogicalType> original_arguments;
	//! Return type of the function
	LogicalType return_type;

public:
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
