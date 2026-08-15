//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/planner/expression/bound_argument_pack.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/function/function.hpp"
#include "duckdb/planner/expression/bound_operator_expression.hpp"

namespace duckdb {

//! An argument pack fills a "*args" or "**kwargs" parameter of a function call: it collects the arguments that the
//! parameter captured and presents them to the function as a single TUPLE (for *args) or STRUCT (for **kwargs) value.
//! It is a BoundOperatorExpression of type ARGUMENT_PACK - not a function call - so that variadic functions can be
//! used to build one without the binder recursing. This struct is only a set of helper methods.
struct ArgumentPack {
	//! Argument pack types carry this reserved alias. Only the binder ever builds one, so the alias is what tells a
	//! pack apart from a TUPLE/STRUCT the caller passed as an ordinary argument - which matters because a bound
	//! function call is serialized as-is, and on deserialization the packs come back already built and must not be
	//! packed a second time.
	static constexpr const char *TYPE_ALIAS = "__argument_pack";

	//! Whether the given expression is an argument pack
	static bool IsPack(const Expression &expr);
	//! Whether the given type is the type of an argument pack
	static bool IsPackType(const LogicalType &type);

	//! The type of a "*args" pack over the given element types: an unnamed TUPLE carrying the pack alias
	static LogicalType PositionalType(vector<LogicalType> element_types);
	//! The type of a "**kwargs" pack: a STRUCT keyed by the caller's argument names, carrying the pack alias
	static LogicalType KeywordType(child_list_t<LogicalType> value_types);

	//! Create an argument pack of the given type, which must have come from PositionalType/KeywordType and have one
	//! child per packed expression. A pack is never NULL itself: NULLs among the packed arguments stay visible as
	//! NULL members, and it is up to the function to decide what they mean.
	static unique_ptr<Expression> Create(vector<unique_ptr<Expression>> children, LogicalType pack_type);
};

} // namespace duckdb
