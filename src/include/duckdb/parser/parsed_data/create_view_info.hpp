//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/parser/parsed_data/create_view_info.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include <string>
#include <unordered_map>

#include "duckdb/parser/parsed_data/create_info.hpp"
#include "duckdb/parser/statement/select_statement.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/types.hpp"
#include "duckdb/common/types/value.hpp"
#include "duckdb/common/unique_ptr.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/winapi.hpp"

namespace duckdb {
class SchemaCatalogEntry;
class ClientContext;
class Deserializer;
class Serializer;

enum class CreateViewBindingMode { BIND_ON_CREATE, SKIP_BINDING };

struct CreateViewInfo : public CreateInfo {
public:
	CreateViewInfo();
	CreateViewInfo(SchemaCatalogEntry &schema, string view_name);
	CreateViewInfo(string catalog_p, string schema_p, string view_name);

public:
	//! View name
	string view_name;
	//! Aliases of the view
	vector<string> aliases;
	//! Return types
	vector<LogicalType> types;
	//! Names of the query
	vector<string> names;
	//! Comments on columns of the query. Note: vector can be empty when no comments are set
	unordered_map<string, Value> column_comments_map;
	//! The SelectStatement of the view
	unique_ptr<SelectStatement> query;
	//! Whether or not to bind the view on create
	CreateViewBindingMode binding_mode = CreateViewBindingMode::BIND_ON_CREATE;

public:
	unique_ptr<CreateInfo> Copy() const override;

	//! Gets a bound CreateViewInfo object from a SELECT statement and a view name, schema name, etc
	DUCKDB_API static unique_ptr<CreateViewInfo> FromSelect(ClientContext &context, unique_ptr<CreateViewInfo> info);
	//! Gets a bound CreateViewInfo object from a CREATE VIEW statement
	DUCKDB_API static unique_ptr<CreateViewInfo> FromCreateView(ClientContext &context, SchemaCatalogEntry &schema,
	                                                            const string &sql);
	//! Parse a SELECT statement from a SQL string
	DUCKDB_API static unique_ptr<SelectStatement> ParseSelect(const string &sql);

	DUCKDB_API void Serialize(Serializer &serializer) const override;
	DUCKDB_API static unique_ptr<CreateInfo> Deserialize(Deserializer &deserializer);

	string ToString() const override;

private:
	CreateViewInfo(vector<string> names, vector<Value> comments, unordered_map<string, Value> column_comments);

	vector<Value> GetColumnCommentsList() const;
};

} // namespace duckdb
