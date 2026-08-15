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

	//! Flatten a bound call's argument packs back into the plain argument list the call was written with, which is
	//! how a call to the same function looked before it took *args/**kwargs. Used when serializing to a storage
	//! format that predates argument packs - reading such a call back simply binds the trailing arguments into
	//! packs again. Only a call shaped like a pre-pack variadic one (leading positional arguments followed by the
	//! "*args" pack) can be put back together that way, so anything else throws. Returns false if the call has no
	//! packs, leaving the outputs untouched.
	static bool Unroll(const vector<unique_ptr<Expression>> &children, const vector<LogicalType> &arguments,
	                   vector<unique_ptr<Expression>> &flat_children, vector<LogicalType> &flat_arguments);
};

} // namespace duckdb
