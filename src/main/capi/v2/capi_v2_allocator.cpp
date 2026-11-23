#include "duckdb/main/capi_v2/capi_v2_internal.hpp"

#include "duckdb/main/extension/capi_allocator.hpp"
#include "duckdb/main/extension_helper.hpp"

#include <cstddef>
#include <cstring>

namespace duckdb {
namespace capiv2 {

inline Allocator *Convert(duckdb_v2_allocator_handle allocator) {
	return reinterpret_cast<Allocator *>(allocator);
}

duckdb_v2_allocator_handle Convert(unique_ptr<Allocator> allocator) {
	return reinterpret_cast<duckdb_v2_allocator_handle>(allocator.release());
}

namespace {

void RegisterExtensionAllocator(Allocator &allocator, duckdb_v2_extension_handle extension) {
	auto &name = GetExtensionLoader(extension).GetRegisteredExtensionName();
	CApiAllocator::Register(allocator, ExtensionHelper::GetExtensionName(name.GetIdentifierName()));
}

void CheckAlignment(idx_t align) {
	if (align == 0 || (align & (align - 1)) != 0) {
		throw InvalidInputException("alignment must be a power of two, got %llu", align);
	}
}

data_ptr_t Allocate(Allocator &allocator, idx_t size, idx_t align, bool zeroed) {
	CheckAlignment(align);
	return size ? CApiAllocator::Allocate(allocator, size, align, zeroed) : nullptr;
}

} // namespace
} // namespace capiv2
} // namespace duckdb

using namespace duckdb::capiv2;

DUCKDB_V2_ERROR duckdb_v2_allocator_get_raw_functions(duckdb_v2_allocator_handle allocator,
                                                      duckdb_v2_allocator_raw_functions *out_functions,
                                                      duckdb_v2_error_info_handle *err) {
	DUCKDB_CHECK_ARG(out_functions);
	*out_functions = {};
	DUCKDB_CHECK_ARG(allocator);
	return WithErrorHandler(err, [&]() {
		auto functions = duckdb::CApiAllocator::GetRawFunctions(*Convert(allocator));
		out_functions->state = functions.state;
		out_functions->alloc = functions.allocate;
		out_functions->alloc_zeroed = functions.allocate_zeroed;
		out_functions->dealloc = functions.free;
		out_functions->realloc = functions.reallocate;
	});
}

DUCKDB_V2_ERROR duckdb_v2_allocator_create_with_extension(duckdb_v2_extension_handle extension,
                                                          duckdb_v2_allocator_handle *out_allocator,
                                                          duckdb_v2_error_info_handle *err) {
	DUCKDB_CHECK_ARG(out_allocator);
	*out_allocator = nullptr;
	return WithErrorHandler(err, [&]() {
		auto allocator = duckdb::CApiAllocator::Create();
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
	    err, [&]() { *out_allocator = Convert(duckdb::CApiAllocator::Create(*Convert(conn)->context->db)); });
}

DUCKDB_V2_ERROR duckdb_v2_allocator_create_with_context(duckdb_v2_context_handle context,
                                                        duckdb_v2_allocator_handle *out_allocator,
                                                        duckdb_v2_error_info_handle *err) {
	DUCKDB_CHECK_ARG(out_allocator);
	*out_allocator = nullptr;
	DUCKDB_CHECK_ARG(context);
	return WithErrorHandler(
	    err, [&]() { *out_allocator = Convert(duckdb::CApiAllocator::Create(*Convert(context)->context.db)); });
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
	return WithErrorHandler(err, [&]() { *out_ptr = Allocate(*Convert(allocator), size, align, false); });
}

DUCKDB_V2_ERROR duckdb_v2_allocator_alloc_zeroed(duckdb_v2_allocator_handle allocator, idx_t size, idx_t align,
                                                 void **out_ptr, duckdb_v2_error_info_handle *err) {
	DUCKDB_CHECK_ARG(out_ptr);
	*out_ptr = nullptr;
	DUCKDB_CHECK_ARG(allocator);
	return WithErrorHandler(err, [&]() { *out_ptr = Allocate(*Convert(allocator), size, align, true); });
}

DUCKDB_V2_ERROR duckdb_v2_allocator_dealloc(duckdb_v2_allocator_handle allocator, void *ptr, idx_t size, idx_t align) {
	duckdb_v2_error_info_handle *err = nullptr;
	DUCKDB_CHECK_ARG(allocator);
	return WithErrorHandler(err, [&]() {
		CheckAlignment(align);
		if (ptr) {
			duckdb::CApiAllocator::Free(*Convert(allocator), static_cast<duckdb::data_ptr_t>(ptr), size, align);
		}
	});
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
		CheckAlignment(align);
		auto &target = *Convert(allocator);
		auto block = static_cast<duckdb::data_ptr_t>(ptr);
		if (!new_size) {
			if (block) {
				duckdb::CApiAllocator::Free(target, block, old_size, align);
			}
			return;
		}
		*out_ptr = block ? duckdb::CApiAllocator::Reallocate(target, block, old_size, align, new_size)
		                 : duckdb::CApiAllocator::Allocate(target, new_size, align, false);
	});
}
