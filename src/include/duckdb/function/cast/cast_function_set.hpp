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

typedef BoundCastInfo (*bind_cast_function_t)(BindCastInput &input, const LogicalType &source,
                                              const LogicalType &target);
typedef int64_t (*implicit_cast_cost_t)(const LogicalType &from, const LogicalType &to);

struct GetCastFunctionInput {
	GetCastFunctionInput(optional_ptr<ClientContext> context = nullptr) : context(context) {
	}
	GetCastFunctionInput(ClientContext &context) : context(&context) {
	}

	optional_ptr<ClientContext> context;
};

struct BindCastFunction {
	BindCastFunction(bind_cast_function_t function,
	                 unique_ptr<BindCastInfo> info = nullptr); // NOLINT: allow implicit cast

	bind_cast_function_t function;
	unique_ptr<BindCastInfo> info;
};

class CastFunctionSet {
public:
	CastFunctionSet();

public:
	DUCKDB_API static CastFunctionSet &Get(ClientContext &context);
	DUCKDB_API static CastFunctionSet &Get(DatabaseInstance &db);

	//! Returns a cast function (from source -> target)
	//! Note that this always returns a function - since a cast is ALWAYS possible if the value is NULL
	DUCKDB_API BoundCastInfo GetCastFunction(const LogicalType &source, const LogicalType &target,
	                                         GetCastFunctionInput &input);
	//! Returns the implicit cast cost of casting from source -> target
	//! -1 means an implicit cast is not possible
	DUCKDB_API int64_t ImplicitCastCost(const LogicalType &source, const LogicalType &target);
	//! Register a new cast function from source to target
	DUCKDB_API void RegisterCastFunction(const LogicalType &source, const LogicalType &target, BoundCastInfo function,
	                                     int64_t implicit_cast_cost = -1);
	DUCKDB_API void RegisterCastFunction(const LogicalType &source, const LogicalType &target,
	                                     bind_cast_function_t bind, int64_t implicit_cast_cost = -1);

private:
	vector<BindCastFunction> bind_functions;
	//! If any custom cast functions have been defined using RegisterCastFunction, this holds the map
	optional_ptr<MapCastInfo> map_info;

private:
	void RegisterCastFunction(const LogicalType &source, const LogicalType &target, MapCastNode node);
};

} // namespace duckdb
