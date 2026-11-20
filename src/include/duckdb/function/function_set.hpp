//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/function/function_set.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/function/function_set_base.hpp"
#include "duckdb/function/aggregate_function.hpp"
#include "duckdb/common/enums/expression_type.hpp"
#include "duckdb/function/scalar_function.hpp"
#include "duckdb/function/table_function.hpp"
#include "duckdb/function/pragma_function.hpp"
#include "duckdb/function/window_function.hpp"

namespace duckdb {

class ScalarFunctionSet : public FunctionSet<ScalarFunction> {
public:
	DUCKDB_API explicit ScalarFunctionSet();
	DUCKDB_API explicit ScalarFunctionSet(Identifier name);
	DUCKDB_API explicit ScalarFunctionSet(ScalarFunction fun);

	DUCKDB_API shared_ptr<const ScalarFunction> GetFunctionByArguments(ClientContext &context,
	                                                                   const vector<LogicalType> &arguments);

	//! Mark every overload in the set as fallible (can throw runtime errors)
	void SetFallible() {
		ApplyToFunctions([](ScalarFunction &fun) { fun.SetFallible(); });
	}
	//! Set the stability of every overload in the set
	void SetStability(FunctionStability stability) {
		ApplyToFunctions([&](ScalarFunction &fun) { fun.SetStability(stability); });
	}

	//! Apply the same per-arg property to every overload in the set.
	void SetArgProperties(idx_t arg_idx, ArgProperties props) {
		ApplyToFunctions([&](ScalarFunction &fun) { fun.SetArgProperties(arg_idx, props); });
	}
	void SetArgProperties(const vector<ArgProperties> &props) {
		ApplyToFunctions([&](ScalarFunction &fun) { fun.SetArgProperties(props); });
	}
	void SetUnaryArgProperties(ArgProperties props) {
		ApplyToFunctions([&](ScalarFunction &fun) { fun.SetUnaryArgProperties(props); });
	}
	void SetStatisticsCallback(function_statistics_t callback) {
		ApplyToFunctions([&](ScalarFunction &fun) { fun.SetStatisticsCallback(callback); });
	}

	//! Declare the set a function language: members of CREATE FUNCTION ... LANGUAGE <name> resolve through it.
	//!
	//! A language lives in the main schema of the system catalog: the catalog rejects a declared language for
	//! another schema. The extension loader places a language its extension registers in its default schema there.
	//!
	//! Registration order is part of the contract: register the declared set before any other overload of the
	//! language. Overloads the extension registers later in its default schema without the declaration, one at a time
	//! or as a set, join the language. Declaring a language after the extension registered an overload of that name
	//! in its own schema fails, since member calls would never select that overload. A binding that registers one
	//! overload per call (the C API) registers the declared overload first
	void DeclareLanguage() {
		is_language = true;
	}
	bool IsLanguage() const {
		return is_language;
	}
	//! The CREATE-time validation of the language's members (if any)
	void SetCreateValidateCallback(scalar_function_create_validate_t callback) {
		create_validate = callback;
	}
	scalar_function_create_validate_t GetCreateValidateCallback() const {
		return create_validate;
	}
	//! Set the language declaration of a set combining the overloads of an existing and an incoming set. This is the
	//! one merge rule of every registration path: the result is a language if either set is, and the incoming set's
	//! validator replaces the existing one, which stays when the incoming set has none
	void MergeLanguageDeclaration(const ScalarFunctionSet &existing, const ScalarFunctionSet &incoming) {
		auto validate = incoming.create_validate ? incoming.create_validate : existing.create_validate;
		is_language = existing.is_language || incoming.is_language;
		create_validate = validate;
	}

private:
	bool is_language = false;
	scalar_function_create_validate_t create_validate = nullptr;
};

class AggregateFunctionSet : public FunctionSet<AggregateFunction> {
public:
	DUCKDB_API explicit AggregateFunctionSet();
	DUCKDB_API explicit AggregateFunctionSet(Identifier name);
	DUCKDB_API explicit AggregateFunctionSet(AggregateFunction fun);

	DUCKDB_API shared_ptr<const AggregateFunction> GetFunctionByArguments(ClientContext &context,
	                                                                      const vector<LogicalType> &arguments);
};

class WindowFunctionSet : public FunctionSet<WindowFunction> {
public:
	DUCKDB_API explicit WindowFunctionSet();
	DUCKDB_API explicit WindowFunctionSet(Identifier name);
	DUCKDB_API explicit WindowFunctionSet(WindowFunction fun);

	DUCKDB_API shared_ptr<const WindowFunction> GetFunctionByArguments(ClientContext &context,
	                                                                   const vector<LogicalType> &arguments);
};

class TableFunctionSet : public FunctionSet<TableFunction> {
public:
	DUCKDB_API explicit TableFunctionSet(Identifier name);
	DUCKDB_API explicit TableFunctionSet(TableFunction fun);

	DUCKDB_API shared_ptr<const TableFunction> GetFunctionByArguments(ClientContext &context,
	                                                                  const vector<LogicalType> &arguments);
};

class PragmaFunctionSet : public FunctionSet<PragmaFunction> {
public:
	DUCKDB_API explicit PragmaFunctionSet(Identifier name);
	DUCKDB_API explicit PragmaFunctionSet(PragmaFunction fun);
};

} // namespace duckdb
