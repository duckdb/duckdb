#include "duckdb/main/capi_v2/capi_v2_internal.hpp"

#include "duckdb/main/database.hpp"
#include "duckdb/storage/object_cache.hpp"

namespace duckdb::capiv2 {

//----------------------------------------------------------------------------------------------------------------------
// Object cache
//----------------------------------------------------------------------------------------------------------------------

//! An entry of the object cache created through the C API: opaque data plus the memory it is accounted as.
class CV2ObjectCacheEntry final : public ObjectCacheEntry {
public:
	//! Wraps data that already exists.
	CV2ObjectCacheEntry(CV2UserData data_p, idx_t size_bytes, idx_t max_size)
	    : data(std::move(data_p)), size_bytes(size_bytes) {
		CheckSize(max_size);
	}

	//! Creates the data through a callback.
	CV2ObjectCacheEntry(duckdb_v2_object_cache_create_callback_fn create, void *create_user_data, idx_t max_size) {
		duckdb_v2_opaque created = {nullptr, nullptr, nullptr};
		idx_t created_size = 0;
		try {
			InvokeWithErrorSlot(
			    [&](duckdb_v2_error_info_handle *err) { create(create_user_data, &created, &created_size, err); });
		} catch (...) {
			// whatever the callback created before failing goes with it
			CV2UserData release(created.ptr, created.destroy, created.equals);
			throw;
		}
		data = CV2UserData(created.ptr, created.destroy, created.equals);
		size_bytes = created_size;
		CheckSize(max_size);
	}

	static string ObjectType() {
		return "capi_v2";
	}
	string GetObjectType() override {
		return ObjectType();
	}
	optional_idx GetEstimatedCacheMemory() const override {
		// no size pins the entry
		return size_bytes == 0 ? optional_idx() : optional_idx(size_bytes);
	}
	void *GetData() const {
		return data.GetData();
	}

private:
	void CheckSize(idx_t max_size) const {
		if (size_bytes > max_size) {
			throw InvalidInputException("Object cache entry of %llu bytes exceeds the cache capacity of %llu bytes",
			                            size_bytes, max_size);
		}
	}

private:
	CV2UserData data;
	idx_t size_bytes = 0;
};

//! The handle of an entry held by a caller: pins the data beyond the cache's own hold on it.
struct CV2ObjectCacheEntryHandle {
	shared_ptr<CV2ObjectCacheEntry> entry;
};

static auto Convert(duckdb_v2_object_cache_entry_handle entry) -> CV2ObjectCacheEntryHandle * {
	return reinterpret_cast<CV2ObjectCacheEntryHandle *>(entry);
}
static auto Convert(CV2ObjectCacheEntryHandle *entry) -> duckdb_v2_object_cache_entry_handle {
	return reinterpret_cast<duckdb_v2_object_cache_entry_handle>(entry);
}

static auto Convert(duckdb_v2_object_cache_handle cache) -> ObjectCache * {
	return reinterpret_cast<ObjectCache *>(cache);
}
static auto Convert(ObjectCache *cache) -> duckdb_v2_object_cache_handle {
	return reinterpret_cast<duckdb_v2_object_cache_handle>(cache);
}

//! Entries are looked up by type and key together; the length prefix keeps the two apart, and the C API prefix keeps
//! them apart from the cache's other users.
static auto CacheKey(const char *type, idx_t type_length, const char *key, idx_t key_length) -> string {
	return StringUtil::Format("capi_v2:%llu:%s:%s", type_length, string(type, type_length), string(key, key_length));
}

static auto CheckedEntry(shared_ptr<ObjectCacheEntry> entry) -> shared_ptr<CV2ObjectCacheEntry> {
	if (!entry || entry->GetObjectType() != CV2ObjectCacheEntry::ObjectType()) {
		throw InvalidInputException("The object cache holds another kind of entry under this key");
	}
	return shared_ptr_cast<ObjectCacheEntry, CV2ObjectCacheEntry>(std::move(entry));
}

static void GiveEntryToCaller(shared_ptr<CV2ObjectCacheEntry> entry, duckdb_v2_object_cache_entry_handle *out) {
	if (!entry) {
		return;
	}
	auto handle = make_uniq<CV2ObjectCacheEntryHandle>();
	handle->entry = std::move(entry);
	*out = Convert(handle.release());
}

} // namespace duckdb::capiv2

//----------------------------------------------------------------------------------------------------------------------
// Public Functions
//----------------------------------------------------------------------------------------------------------------------

using namespace duckdb::capiv2;

DUCKDB_V2_ERROR duckdb_v2_instance_get_object_cache(duckdb_v2_instance_handle instance,
                                                    duckdb_v2_object_cache_handle *cache,
                                                    duckdb_v2_error_info_handle *err) {
	DUCKDB_CHECK_ARG(instance);
	DUCKDB_CHECK_ARG(cache);
	*cache = nullptr;
	return WithErrorHandler(err,
	                        [&]() { *cache = Convert(&Convert(instance)->GetDatabase().instance->GetObjectCache()); });
}

DUCKDB_V2_ERROR duckdb_v2_context_get_object_cache(duckdb_v2_context_handle context,
                                                   duckdb_v2_object_cache_handle *cache,
                                                   duckdb_v2_error_info_handle *err) {
	DUCKDB_CHECK_ARG(context);
	DUCKDB_CHECK_ARG(cache);
	*cache = nullptr;
	return WithErrorHandler(err, [&]() {
		*cache = Convert(&duckdb::DatabaseInstance::GetDatabase(Convert(context)->context).GetObjectCache());
	});
}

DUCKDB_V2_ERROR duckdb_v2_object_cache_put(duckdb_v2_object_cache_handle cache, const char *type, idx_t type_length,
                                           const char *key, idx_t key_length, duckdb_v2_opaque *data, idx_t size_bytes,
                                           duckdb_v2_error_info_handle *err) {
	DUCKDB_CHECK_ARG(data);
	// owned from here, so every failure below destroys the data
	CV2UserData owned(data->ptr, data->destroy, data->equals);
	DUCKDB_CHECK_ARG(cache);
	DUCKDB_CHECK_STR_ARG(type, type_length);
	DUCKDB_CHECK_STR_ARG(key, key_length);
	return WithErrorHandler(err, [&]() {
		auto &object_cache = *Convert(cache);
		auto entry =
		    duckdb::make_shared_ptr<CV2ObjectCacheEntry>(std::move(owned), size_bytes, object_cache.GetMaxMemory());
		object_cache.Put(CacheKey(type, type_length, key, key_length), std::move(entry));
	});
}

DUCKDB_V2_ERROR duckdb_v2_object_cache_get(duckdb_v2_object_cache_handle cache, const char *type, idx_t type_length,
                                           const char *key, idx_t key_length,
                                           duckdb_v2_object_cache_entry_handle *entry,
                                           duckdb_v2_error_info_handle *err) {
	DUCKDB_CHECK_ARG(cache);
	DUCKDB_CHECK_STR_ARG(type, type_length);
	DUCKDB_CHECK_STR_ARG(key, key_length);
	DUCKDB_CHECK_ARG(entry);
	*entry = nullptr;
	return WithErrorHandler(err, [&]() {
		auto found = Convert(cache)->Get<CV2ObjectCacheEntry>(CacheKey(type, type_length, key, key_length));
		GiveEntryToCaller(std::move(found), entry);
	});
}

DUCKDB_V2_ERROR duckdb_v2_object_cache_get_or_create(duckdb_v2_object_cache_handle cache, const char *type,
                                                     idx_t type_length, const char *key, idx_t key_length,
                                                     duckdb_v2_object_cache_create_callback_fn create,
                                                     void *create_user_data, duckdb_v2_object_cache_entry_handle *entry,
                                                     duckdb_v2_error_info_handle *err) {
	DUCKDB_CHECK_ARG(cache);
	DUCKDB_CHECK_STR_ARG(type, type_length);
	DUCKDB_CHECK_STR_ARG(key, key_length);
	DUCKDB_CHECK_ARG(create);
	DUCKDB_CHECK_ARG(entry);
	*entry = nullptr;
	return WithErrorHandler(err, [&]() {
		auto &object_cache = *Convert(cache);
		auto cache_key = CacheKey(type, type_length, key, key_length);
		auto found = object_cache.GetOrCreateObject(cache_key, [&]() -> duckdb::shared_ptr<duckdb::ObjectCacheEntry> {
			return duckdb::make_shared_ptr<CV2ObjectCacheEntry>(create, create_user_data, object_cache.GetMaxMemory());
		});
		GiveEntryToCaller(CheckedEntry(std::move(found)), entry);
	});
}

DUCKDB_V2_ERROR duckdb_v2_object_cache_delete(duckdb_v2_object_cache_handle cache, const char *type, idx_t type_length,
                                              const char *key, idx_t key_length, duckdb_v2_error_info_handle *err) {
	DUCKDB_CHECK_ARG(cache);
	DUCKDB_CHECK_STR_ARG(type, type_length);
	DUCKDB_CHECK_STR_ARG(key, key_length);
	return WithErrorHandler(err, [&]() { Convert(cache)->Delete(CacheKey(type, type_length, key, key_length)); });
}

DUCKDB_V2_ERROR duckdb_v2_object_cache_entry_get_data(duckdb_v2_object_cache_entry_handle entry, void **data,
                                                      duckdb_v2_error_info_handle *err) {
	DUCKDB_CHECK_ARG(entry);
	DUCKDB_CHECK_ARG(data);
	return WithErrorHandler(err, [&]() { *data = Convert(entry)->entry->GetData(); });
}

DUCKDB_V2_ERROR duckdb_v2_object_cache_entry_destroy(duckdb_v2_object_cache_entry_handle *entry) {
	if (!entry) {
		return DUCKDB_V2_ERROR_NONE;
	}
	return WithErrorHandler(nullptr, [&]() {
		duckdb::unique_ptr<CV2ObjectCacheEntryHandle> owned(Convert(*entry));
		*entry = nullptr;
	});
}
