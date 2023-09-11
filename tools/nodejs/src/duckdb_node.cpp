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

	exports.DefineProperties({
	    DEFINE_CONSTANT_INTEGER(exports, node_duckdb::Database::DUCKDB_NODEJS_ERROR, ERROR) DEFINE_CONSTANT_INTEGER(
	        exports, node_duckdb::Database::DUCKDB_NODEJS_READONLY, OPEN_READONLY) // same as SQLite
	    DEFINE_CONSTANT_INTEGER(exports, 0, OPEN_READWRITE)                        // ignored
	    DEFINE_CONSTANT_INTEGER(exports, 0, OPEN_CREATE)                           // ignored
	    DEFINE_CONSTANT_INTEGER(exports, 0, OPEN_FULLMUTEX)                        // ignored
	    DEFINE_CONSTANT_INTEGER(exports, 0, OPEN_SHAREDCACHE)                      // ignored
	    DEFINE_CONSTANT_INTEGER(exports, 0, OPEN_PRIVATECACHE)                     // ignored
	});
}

NODE_API_ADDON(NodeDuckDB);
