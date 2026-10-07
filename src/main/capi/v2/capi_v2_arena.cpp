#include "duckdb/main/capi_v2/capi_v2_internal.hpp"

#include "duckdb/common/types/string_heap.hpp"
#include "duckdb/common/types/vector.hpp"
#include "duckdb/common/vector/string_vector.hpp"

namespace duckdb {
namespace capiv2 {

auto Convert(ArenaAllocator *allocator) -> duckdb_v2_arena_handle {
	return reinterpret_cast<duckdb_v2_arena_handle>(allocator);
}
auto Convert(duckdb_v2_arena_handle arena) -> ArenaAllocator * {
	return reinterpret_cast<ArenaAllocator *>(arena);
}

} // namespace capiv2
} // namespace duckdb

//----------------------------------------------------------------------------------------------------------------------
// Public API
//----------------------------------------------------------------------------------------------------------------------

using namespace duckdb::capiv2;

DUCKDB_V2_ERROR duckdb_v2_vector_get_arena(duckdb_v2_vector_handle vector, duckdb_v2_arena_handle *out_arena,
                                           duckdb_v2_error_info_handle *err) {
	DUCKDB_CHECK_ARG(out_arena);
	*out_arena = nullptr;
	DUCKDB_CHECK_ARG(vector);
	return WithErrorHandler(err, [&]() {
		auto *vec = Convert(vector);

		// Physical VARCHAR backs VARCHAR / BLOB / BIT / BIGNUM
		if (vec->GetType().InternalType() != duckdb::PhysicalType::VARCHAR) {
			throw duckdb::InvalidInputException("duckdb_v2_vector_get_arena: vector is not a string/blob-backed type");
		}
		auto &heap = duckdb::StringVector::GetStringHeap(*vec);
		*out_arena = Convert(&heap.GetAllocator());
	});
}

DUCKDB_V2_ERROR duckdb_v2_arena_allocate(duckdb_v2_arena_handle arena, idx_t byte_len, uint8_t **out_ptr,
                                         duckdb_v2_error_info_handle *err) {
	DUCKDB_CHECK_ARG(out_ptr);
	*out_ptr = nullptr;
	DUCKDB_CHECK_ARG(arena);
	return WithErrorHandler(err, [&]() { *out_ptr = Convert(arena)->Allocate(byte_len); });
}
