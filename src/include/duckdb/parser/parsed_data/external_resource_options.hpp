//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/parser/parsed_data/external_resource_options.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/case_insensitive_map.hpp"
#include "duckdb/common/identifier.hpp"
#include "duckdb/common/types/value.hpp"
#include "duckdb/common/unordered_map.hpp"
#include "duckdb/parser/parsed_expression.hpp"

namespace duckdb {
class Serializer;
class Deserializer;

//! The `EXTERNAL RESOURCE '<type>' [AS r] [(params)]` clause of a `ATTACH/CONNECT TO EXTERNAL RESOURCE ...
//! ATTACH|CONNECT` statement. Owned by AttachInfo/ConnectInfo. Separates the resource TYPE (the recipe/registry key)
//! from its create PARAMS, which is why provider is its own field rather than a magic key inside the params.
struct ExternalResourceOptions {
	//! Create params (key -> expression); transient — consumed at bind, then `params` holds them.
	case_insensitive_map_t<unique_ptr<ParsedExpression>> parsed_params;
	//! Optional resource alias (`AS r`). Currently informational (a scoped resource is anonymous;
	//! naming becomes meaningful with the instance registry).
	Identifier alias;
	//! The resource type (provider) — the registered external-resource-type name to provision. A string
	//! literal, so it is fixed at parse time.
	string provider;
	//! Bound create params forwarded to the type's create function as a MAP(VARCHAR, VARCHAR).
	unordered_map<string, Value> params;
	//! Reference form: when the resource clause is a bare identifier (not a string type), this is the
	//! name of an already-registered resource to BORROW (attach/connect to it without owning teardown),
	//! rather than provisioning a new one. Mutually exclusive with `provider`.
	string reference_name;

	unique_ptr<ExternalResourceOptions> Copy() const;
	//! Renders `EXTERNAL RESOURCE '<type>' [AS r] (k v, ...)` (without the leading WITH or trailing verb).
	string ToString() const;

	void Serialize(Serializer &serializer) const;
	static unique_ptr<ExternalResourceOptions> Deserialize(Deserializer &deserializer);
};

} // namespace duckdb
