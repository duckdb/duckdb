#include "duckdb/main/capi_v2/capi_v2_internal.hpp"

#include "duckdb/main/extension/capi_allocator.hpp"
#include "duckdb/main/extension_helper.hpp"

#include <cstddef>
#include <cstring>

namespace duckdb {
namespace capiv2 {

Allocator *Convert(duckdb_v2_allocator_handle allocator) {
	return reinterpret_cast<Allocator *>(allocator);
}

duckdb_v2_allocator_handle Convert(unique_ptr<Allocator> allocator) {
	return reinterpret_cast<duckdb_v2_allocator_handle>(allocator.release());
}

namespace {

void RegisterExtensionAllocator(Allocator &allocator, duckdb_v2_extension_handle extension) {
	auto &name = GetExtensionLoader(extension).GetRegisteredExtensionName();
	ExtensionAllocator::Register(allocator, ExtensionHelper::GetExtensionName(name.GetIdentifierName()));
}

void CheckAlignment(idx_t align) {
	if (align == 0 || !IsPowerOfTwo(align)) {
		throw InvalidInputException("alignment must be a power of two, got %llu", align);
	}
}

idx_t BackingSize(idx_t size, idx_t align) {
	const auto limit = MinValue<idx_t>(Allocator::MAXIMUM_ALLOC_SIZE, NumericLimits<uintptr_t>::Maximum());
	if (align >= limit || size >= limit - align) {
		throw OutOfMemoryException("Failed to allocate block of %llu bytes aligned to %llu (bad allocation)", size,
		                           align);
	}
	return align > alignof(std::max_align_t) ? size + align : MaxValue(size, align);
}

data_ptr_t ResizeAligned(Allocator &allocator, data_ptr_t ptr, idx_t old_size, idx_t align, idx_t size) {
	CheckAlignment(align);
	const bool over_aligned = align > alignof(std::max_align_t);
	auto backing = ptr && over_aligned ? Load<data_ptr_t>(ptr - sizeof(data_ptr_t)) : ptr;
	if (!size) {
		if (ptr) {
			allocator.FreeData(backing, BackingSize(old_size, align));
		}
		return nullptr;
	}
	auto backing_size = BackingSize(size, align);
	auto old_offset = ptr ? idx_t(ptr - backing) : 0;
	backing = ptr ? allocator.ReallocateData(backing, BackingSize(old_size, align), backing_size)
	              : allocator.AllocateData(backing_size);
	if (!over_aligned) {
		return backing;
	}
	// realloc preserves the backing bytes, but may move the payload to a different alignment offset.
	auto address = reinterpret_cast<uintptr_t>(backing + sizeof(data_ptr_t));
	auto aligned = reinterpret_cast<data_ptr_t>(AlignValue<uintptr_t>(address, align));
	if (old_size && aligned != backing + old_offset) {
		memmove(aligned, backing + old_offset, MinValue(size, old_size));
	}
	Store<data_ptr_t>(backing, aligned - sizeof(data_ptr_t));
	return aligned;
}

} // namespace
} // namespace capiv2
} // namespace duckdb

using namespace duckdb::capiv2;

DUCKDB_V2_ERROR duckdb_v2_allocator_create_with_extension(duckdb_v2_extension_handle extension,
                                                          duckdb_v2_allocator_handle *out_allocator,
                                                          duckdb_v2_error_info_handle *err) {
	DUCKDB_CHECK_ARG(out_allocator);
	*out_allocator = nullptr;
	return WithErrorHandler(err, [&]() {
		auto allocator = duckdb::ExtensionAllocator::Create();
		if (extension) {
			RegisterExtensionAllocator(*allocator, extension);
		}
		*out_allocator = Convert(std::move(allocator));
	});
}

DUCKDB_V2_ERROR duckdb_v2_allocator_register_extension(duckdb_v2_allocator_handle allocator,
                                                       duckdb_v2_extension_handle extension,
                                                       duckdb_v2_error_info_handle *err) {
	DUCKDB_CHECK_ARG(allocator);
	DUCKDB_CHECK_ARG(extension);
	return WithErrorHandler(err, [&]() { RegisterExtensionAllocator(*Convert(allocator), extension); });
}

DUCKDB_V2_ERROR duckdb_v2_allocator_create_with_connection(duckdb_v2_connection_handle conn,
                                                           duckdb_v2_allocator_handle *out_allocator,
                                                           duckdb_v2_error_info_handle *err) {
	DUCKDB_CHECK_ARG(out_allocator);
	*out_allocator = nullptr;
	DUCKDB_CHECK_ARG(conn);
	return WithErrorHandler(
	    err, [&]() { *out_allocator = Convert(duckdb::ExtensionAllocator::Create(*Convert(conn)->context->db)); });
}

DUCKDB_V2_ERROR duckdb_v2_allocator_create_with_context(duckdb_v2_context_handle context,
                                                        duckdb_v2_allocator_handle *out_allocator,
                                                        duckdb_v2_error_info_handle *err) {
	DUCKDB_CHECK_ARG(out_allocator);
	*out_allocator = nullptr;
	DUCKDB_CHECK_ARG(context);
	return WithErrorHandler(
	    err, [&]() { *out_allocator = Convert(duckdb::ExtensionAllocator::Create(*Convert(context)->context.db)); });
}

DUCKDB_V2_ERROR duckdb_v2_allocator_destroy(duckdb_v2_allocator_handle *allocator) {
	return WithErrorHandler(nullptr, [&]() {
		if (allocator) {
			delete Convert(*allocator);
			*allocator = nullptr;
		}
	});
}

DUCKDB_V2_ERROR duckdb_v2_allocator_alloc(duckdb_v2_allocator_handle allocator, idx_t size, idx_t align, void **out_ptr,
                                          duckdb_v2_error_info_handle *err) {
	DUCKDB_CHECK_ARG(out_ptr);
	*out_ptr = nullptr;
	DUCKDB_CHECK_ARG(allocator);
	return WithErrorHandler(err, [&]() { *out_ptr = ResizeAligned(*Convert(allocator), nullptr, 0, align, size); });
}

DUCKDB_V2_ERROR duckdb_v2_allocator_alloc_zeroed(duckdb_v2_allocator_handle allocator, idx_t size, idx_t align,
                                                 void **out_ptr, duckdb_v2_error_info_handle *err) {
	auto rc = duckdb_v2_allocator_alloc(allocator, size, align, out_ptr, err);
	if (rc == DUCKDB_V2_ERROR_NONE && size) {
		memset(*out_ptr, 0, size);
	}
	return rc;
}

DUCKDB_V2_ERROR duckdb_v2_allocator_dealloc(duckdb_v2_allocator_handle allocator, void *ptr, idx_t size, idx_t align) {
	duckdb_v2_error_info_handle *err = nullptr;
	DUCKDB_CHECK_ARG(allocator);
	return WithErrorHandler(
	    err, [&]() { ResizeAligned(*Convert(allocator), static_cast<duckdb::data_ptr_t>(ptr), size, align, 0); });
}

DUCKDB_V2_ERROR duckdb_v2_allocator_realloc(duckdb_v2_allocator_handle allocator, void *ptr, idx_t old_size,
                                            idx_t align, idx_t new_size, void **out_ptr,
                                            duckdb_v2_error_info_handle *err) {
	DUCKDB_CHECK_ARG(out_ptr);
	*out_ptr = nullptr;
	DUCKDB_CHECK_ARG(allocator);
	return WithErrorHandler(err, [&]() {
		if ((ptr == nullptr) != (old_size == 0)) {
			throw duckdb::InvalidInputException("ptr must be NULL exactly when old_size is 0");
		}
		*out_ptr = ResizeAligned(*Convert(allocator), static_cast<duckdb::data_ptr_t>(ptr), old_size, align, new_size);
	});
}
