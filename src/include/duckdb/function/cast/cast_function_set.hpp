//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/function/cast/cast_function_set.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/function/cast/default_casts.hpp"

namespace duckdb {
struct MapCastInfo;
struct MapCastNode;
struct DBConfig;

typedef BoundCastInfo (*bind_cast_function_t)(BindCastInput &input, const LogicalType &source,
                                              const LogicalType &target);
typedef int64_t (*implicit_cast_cost_t)(const LogicalType &from, const LogicalType &to);

class LogicalTypeResolver {
public:
	explicit LogicalTypeResolver(optional_ptr<ClientContext> context_p) : context(context_p) {
	}
	virtual ~LogicalTypeResolver() = default;

	virtual bool Operation(const LogicalType &left, const LogicalType &right, LogicalType &result) = 0;

public:
	optional_ptr<ClientContext> context;
};

typedef bool (*combine_types_rule_function_t)(LogicalTypeResolver &resolver, const LogicalType &left,
                                              const LogicalType &right, LogicalType &result);

struct CombineTypesRule {
	bool (*matches)(const LogicalType &left, const LogicalType &right); // order-insensitive in (left, right)
	combine_types_rule_function_t function;
};

struct GetCastFunctionInput {
	explicit GetCastFunctionInput(optional_ptr<ClientContext> context = nullptr) : context(context) {
	}
	explicit GetCastFunctionInput(ClientContext &context) : context(&context) {
	}

	optional_ptr<ClientContext> context;
	optional_idx query_location;
};

struct BindCastFunction {
	BindCastFunction(bind_cast_function_t function, // NOLINT: allow implicit cast
	                 unique_ptr<BindCastInfo> info = nullptr);

	bind_cast_function_t function;
	unique_ptr<BindCastInfo> info;
};

class CastFunctionSet {
public:
	CastFunctionSet();
	explicit CastFunctionSet(DBConfig &config);

public:
	DUCKDB_API static CastFunctionSet &Get(ClientContext &context);
	DUCKDB_API static CastFunctionSet &Get(DatabaseInstance &db);

	//! Returns a cast function (from source -> target)
	//! Note that this always returns a function - since a cast is ALWAYS possible if the value is NULL
	DUCKDB_API BoundCastInfo GetCastFunction(const LogicalType &source, const LogicalType &target,
	                                         GetCastFunctionInput &input);
	//! Returns the implicit cast cost of casting from source -> target
	//! -1 means an implicit cast is not possible
	DUCKDB_API int64_t ImplicitCastCost(optional_ptr<ClientContext> context, const LogicalType &source,
	                                    const LogicalType &target);
	DUCKDB_API static int64_t ImplicitCastCost(ClientContext &context, const LogicalType &source,
	                                           const LogicalType &target);
	DUCKDB_API static int64_t ImplicitCastCost(DatabaseInstance &db, const LogicalType &source,
	                                           const LogicalType &target);
	//! Register a new cast function from source to target
	DUCKDB_API void RegisterCastFunction(const LogicalType &source, const LogicalType &target, BoundCastInfo function,
	                                     int64_t implicit_cast_cost = -1);
	DUCKDB_API void RegisterCastFunction(const LogicalType &source, const LogicalType &target,
	                                     bind_cast_function_t bind, int64_t implicit_cast_cost = -1);

	//! Register a combine rule for LogicalType::TryGetMaxLogicalType. Registered rules are consulted before the
	//! built-in rules, most-recently-registered first. Equal rules fire when both type ids match, unequal when they
	//! differ.
	DUCKDB_API void RegisterCombineEqualTypesRule(CombineTypesRule rule);
	DUCKDB_API void RegisterCombineUnequalTypesRule(CombineTypesRule rule);
	//! Apply the first matching registered rule. Returns true if a rule matched (its result is written to `success`),
	//! false if none matched and the caller should fall back to the built-in rules.
	bool TryCombineEqualTypes(LogicalTypeResolver &resolver, const LogicalType &left, const LogicalType &right,
	                          LogicalType &result, bool &success);
	bool TryCombineUnequalTypes(LogicalTypeResolver &resolver, const LogicalType &left, const LogicalType &right,
	                            LogicalType &result, bool &success);

private:
	optional_ptr<DBConfig> config;
	vector<BindCastFunction> bind_functions;
	//! If any custom cast functions have been defined using RegisterCastFunction, this holds the map
	optional_ptr<MapCastInfo> map_info;
	vector<CombineTypesRule> combine_equal_rules;
	vector<CombineTypesRule> combine_unequal_rules;

private:
	void RegisterCastFunction(const LogicalType &source, const LogicalType &target, MapCastNode node);
};

} // namespace duckdb
