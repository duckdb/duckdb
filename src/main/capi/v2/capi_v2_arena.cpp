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
	return WithErrorHandler(err, [&]() {
		if (!out_arena) {
			throw duckdb::InvalidInputException("null argument to duckdb_v2_vector_get_arena");
		}
		*out_arena = nullptr;
		if (!vector) {
			throw duckdb::InvalidInputException("null argument to duckdb_v2_vector_get_arena");
		}
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
	return WithErrorHandler(err, [&]() {
		if (!out_ptr) {
			throw duckdb::InvalidInputException("null argument to duckdb_v2_arena_allocate");
		}
		*out_ptr = nullptr;
		if (!arena) {
			throw duckdb::InvalidInputException("null argument to duckdb_v2_arena_allocate");
		}
		*out_ptr = Convert(arena)->Allocate(byte_len);
	});
}
