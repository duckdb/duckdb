#include "duckdb/storage/object_cache.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/main/database.hpp"

namespace duckdb {

ObjectCache &ObjectCache::GetObjectCache(ClientContext &context) {
	return *context.db->object_cache;
}

}

