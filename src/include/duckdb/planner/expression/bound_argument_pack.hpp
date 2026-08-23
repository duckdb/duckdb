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

	static const vector<Vector> &GetInput(const Vector &pack);

	static const child_list_t<LogicalType> &GetTypes(const LogicalType &pack);

	static idx_t GetSize(const LogicalType &pack);

	//! Whether the given type is the type of an argument pack
	static bool IsPackType(const LogicalType &type);

	//! Whether the given expression is still a pack expression. A pack is built by the binder, but the optimizer
	//! may replace it with an expression of the same type - a constant, say - that no longer holds the arguments.
	static bool IsPack(const Expression &expr);

	//! The type of a "*args" pack over the given element types: an unnamed TUPLE carrying the pack alias
	static LogicalType PositionalType(vector<LogicalType> element_types);
	//! The type of a "**kwargs" pack: a STRUCT keyed by the caller's argument names, carrying the pack alias
	static LogicalType KeywordType(child_list_t<LogicalType> value_types);

	//! Create an argument pack of the given type, which must have come from PositionalType/KeywordType and have one
	//! child per packed expression. A pack is never NULL itself: NULLs among the packed arguments stay visible as
	//! NULL members, and it is up to the function to decide what they mean.
	static unique_ptr<Expression> Create(vector<unique_ptr<Expression>> children, LogicalType pack_type);

	//! Flatten a bound call's argument packs back into the plain argument list the call was written with, so that
	//! packs never reach disk - Reroll builds them again when the call is read back. A "**kwargs" pack is
	//! flattened with its field names in the expression aliases. Only a call with a single trailing pack can be
	//! put back together that way: anything else is written as-is, unless the storage version predates argument
	//! packs, in which case it cannot be represented at all and this throws. Returns false when nothing was
	//! unrolled, leaving the outputs untouched.
	static bool Unroll(Serializer &serializer, const vector<unique_ptr<Expression>> &children,
	                   const vector<LogicalType> &arguments, vector<unique_ptr<Expression>> &flat_children,
	                   vector<LogicalType> &flat_arguments);

	//! The inverse of Unroll: collect the trailing arguments of a call read back from disk into the pack the
	//! signature declares. A "**kwargs" pack is keyed by the expression aliases, which is where Unroll left the
	//! field names. Returns false when there is nothing to roll up.
	static bool Reroll(const FunctionSignature &sig, vector<unique_ptr<Expression>> &children,
	                   vector<LogicalType> &arguments);

	//! The collected arguments, in place. A pack is built by the binder and only replaced afterwards, by the
	//! optimizer, which the binder undoes before re-binding a call - so this is always reachable from a bind
	//! callback.
	static vector<unique_ptr<Expression>> &GetPackedChildren(Expression &pack);

	//! Recompute the pack's type from the arguments it currently holds. A bind callback that rewrites them - by
	//! casting one, say - has to call this so that the pack still describes what it holds.
	static void RefreshType(Expression &pack);
};

} // namespace duckdb
