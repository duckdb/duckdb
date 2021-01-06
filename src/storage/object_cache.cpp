#include "duckdb/storage/object_cache.hpp"

namespace duckdb {

ObjectCache &ObjectCache::GetObjectCache(ClientContext &context) {
	return *context.db->object_cache;
}

}

