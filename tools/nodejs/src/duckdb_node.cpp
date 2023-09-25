#include "duckdb_node.hpp"

#define DEFINE_CONSTANT_INTEGER(target, constant, name)                                                                \
	Napi::PropertyDescriptor::Value(#name, Napi::Number::New(env, constant),                                           \
	                                static_cast<napi_property_attributes>(napi_enumerable | napi_configurable)),

NodeDuckDB::NodeDuckDB(Napi::Env env, Napi::Object exports) {
	Napi::HandleScope scope(env);

	database_constructor = node_duckdb::Database::Init(env, exports);
	connection_constructor = node_duckdb::Connection::Init(env, exports);
	statement_constructor = node_duckdb::Statement::Init(env, exports);
	query_result_constructor = node_duckdb::QueryResult::Init(env, exports);

	auto token_type_enum = Napi::Object::New(env);

	token_type_enum.Set("IDENTIFIER", 0);
	token_type_enum.Set("NUMERIC_CONSTANT", 1);
	token_type_enum.Set("STRING_CONSTANT", 2);
	token_type_enum.Set("OPERATOR", 3);
	token_type_enum.Set("KEYWORD", 4);
	token_type_enum.Set("COMMENT", 5);

	// TypeScript enums expose an inverse mapping.
	token_type_enum.Set((uint32_t)0, "IDENTIFIER");
	token_type_enum.Set((uint32_t)1, "NUMERIC_CONSTANT");
	token_type_enum.Set((uint32_t)2, "STRING_CONSTANT");
	token_type_enum.Set((uint32_t)3, "OPERATOR");
	token_type_enum.Set((uint32_t)4, "KEYWORD");
	token_type_enum.Set((uint32_t)5, "COMMENT");

	token_type_enum_ref = Napi::ObjectReference::New(token_type_enum);

	exports.DefineProperties(
	    {DEFINE_CONSTANT_INTEGER(exports, node_duckdb::Database::DUCKDB_NODEJS_ERROR, ERROR) DEFINE_CONSTANT_INTEGER(
	        exports, node_duckdb::Database::DUCKDB_NODEJS_READONLY, OPEN_READONLY) // same as SQLite
	     DEFINE_CONSTANT_INTEGER(exports, 0, OPEN_READWRITE)                       // ignored
	     DEFINE_CONSTANT_INTEGER(exports, 0, OPEN_CREATE)                          // ignored
	     DEFINE_CONSTANT_INTEGER(exports, 0, OPEN_FULLMUTEX)                       // ignored
	     DEFINE_CONSTANT_INTEGER(exports, 0, OPEN_SHAREDCACHE)                     // ignored
	     DEFINE_CONSTANT_INTEGER(exports, 0, OPEN_PRIVATECACHE)                    // ignored

	     Napi::PropertyDescriptor::Value("TokenType", token_type_enum,
	                                     static_cast<napi_property_attributes>(napi_enumerable | napi_configurable))});
}

NODE_API_ADDON(NodeDuckDB);
