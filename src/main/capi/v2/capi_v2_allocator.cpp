#include "duckdb/main/capi_v2/capi_v2_internal.hpp"

#include "duckdb/main/extension/capi_allocator.hpp"
#include "duckdb/main/extension_manager.hpp"

#include <cstring>

namespace duckdb {
namespace capiv2 {

auto Convert(duckdb_v2_allocator_handle allocator) -> CApiAllocator * {
	return reinterpret_cast<CApiAllocator *>(allocator);
}
auto Convert(CApiAllocator *allocator) -> duckdb_v2_allocator_handle {
	return reinterpret_cast<duckdb_v2_allocator_handle>(allocator);
}

namespace {

void CheckAlignment(idx_t align) {
	if (align == 0 || !IsPowerOfTwo(align)) {
		throw InvalidInputException("alignment must be a power of two, got %llu", align);
	}
}

} // namespace
} // namespace capiv2
} // namespace duckdb

//----------------------------------------------------------------------------------------------------------------------
// Public API
//----------------------------------------------------------------------------------------------------------------------

using namespace duckdb::capiv2;

DUCKDB_V2_ERROR duckdb_v2_extension_get_allocator(duckdb_v2_extension_handle extension,
                                                  duckdb_v2_allocator_handle *out_allocator,
                                                  duckdb_v2_error_info_handle *err) {
	DUCKDB_CHECK_ARG(out_allocator);
	*out_allocator = nullptr;
	DUCKDB_CHECK_ARG(extension);
	return WithErrorHandler(err, [&]() {
		auto &loader = GetExtensionLoader(extension);
		auto &allocator = duckdb::CApiAllocator::Get(loader.GetRegisteredExtensionName().GetIdentifierName());
		duckdb::ExtensionManager::Get(loader.GetDatabaseInstance()).AttachAllocator(allocator);
		*out_allocator = Convert(&allocator);
	});
}

DUCKDB_V2_ERROR duckdb_v2_alloc(duckdb_v2_allocator_handle allocator, idx_t size, idx_t align, void **out_ptr,
                                duckdb_v2_error_info_handle *err) {
	DUCKDB_CHECK_ARG(out_ptr);
	*out_ptr = nullptr;
	DUCKDB_CHECK_ARG(allocator);
	return WithErrorHandler(err, [&]() {
		CheckAlignment(align);
		if (size > 0) {
			*out_ptr = Convert(allocator)->AllocateAligned(size, align);
		}
	});
}

DUCKDB_V2_ERROR duckdb_v2_alloc_zeroed(duckdb_v2_allocator_handle allocator, idx_t size, idx_t align, void **out_ptr,
                                       duckdb_v2_error_info_handle *err) {
	DUCKDB_CHECK_ARG(out_ptr);
	*out_ptr = nullptr;
	DUCKDB_CHECK_ARG(allocator);
	return WithErrorHandler(err, [&]() {
		CheckAlignment(align);
		if (size > 0) {
			auto result = Convert(allocator)->AllocateAligned(size, align);
			memset(result, 0, size);
			*out_ptr = result;
		}
	});
}

DUCKDB_V2_ERROR duckdb_v2_dealloc(duckdb_v2_allocator_handle allocator, void *ptr, idx_t size, idx_t align) {
	duckdb_v2_error_info_handle *err = nullptr;
	DUCKDB_CHECK_ARG(allocator);
	return WithErrorHandler(err, [&]() {
		CheckAlignment(align);
		if (ptr) {
			Convert(allocator)->FreeAligned(static_cast<duckdb::data_ptr_t>(ptr), size, align);
		}
	});
}

DUCKDB_V2_ERROR duckdb_v2_realloc(duckdb_v2_allocator_handle allocator, void *ptr, idx_t old_size, idx_t align,
                                  idx_t new_size, void **out_ptr, duckdb_v2_error_info_handle *err) {
	DUCKDB_CHECK_ARG(out_ptr);
	*out_ptr = nullptr;
	DUCKDB_CHECK_ARG(allocator);
	return WithErrorHandler(err, [&]() {
		CheckAlignment(align);
		if ((ptr == nullptr) != (old_size == 0)) {
			throw duckdb::InvalidInputException("ptr must be NULL exactly when old_size is 0");
		}
		auto &alloc = *Convert(allocator);
		auto pointer = static_cast<duckdb::data_ptr_t>(ptr);
		if (!pointer) {
			if (new_size > 0) {
				*out_ptr = alloc.AllocateAligned(new_size, align);
			}
		} else if (new_size == 0) {
			alloc.FreeAligned(pointer, old_size, align);
		} else {
			*out_ptr = alloc.ReallocateAligned(pointer, old_size, align, new_size);
		}
	});
}
