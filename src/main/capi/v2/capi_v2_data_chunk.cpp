#include "duckdb/main/capi_v2/capi_v2_internal.hpp"

#include "duckdb/common/allocator.hpp"
#include "duckdb/common/type_visitor.hpp"

using namespace duckdb::capiv2;

DUCKDB_V2_ERROR duckdb_v2_data_chunk_create(const duckdb_v2_logical_type_handle *types, idx_t column_count,
                                            duckdb_v2_data_chunk_handle *out_chunk, duckdb_v2_error_info_handle *err) {
	return WithErrorHandler(err, [&]() {
		if (!out_chunk) {
			throw duckdb::InvalidInputException("null argument to duckdb_v2_data_chunk_create");
		}
		*out_chunk = nullptr;
		if (!types) {
			throw duckdb::InvalidInputException("null argument to duckdb_v2_data_chunk_create");
		}
		duckdb::vector<duckdb::LogicalType> logical_types;
		logical_types.reserve(column_count);
		for (idx_t i = 0; i < column_count; i++) {
			if (!types[i]) {
				throw duckdb::InvalidInputException("null logical type at index %llu", i);
			}
			const auto &ltype = *Convert(types[i]);
			// ANY is a signature wildcard with no physical layout; a chunk allocates
			// storage, so reject it (an ANY vector throws InternalException).
			if (duckdb::TypeVisitor::Contains(ltype, duckdb::LogicalTypeId::ANY)) {
				throw duckdb::InvalidInputException("logical type at index %llu cannot be ANY", i);
			}
			logical_types.push_back(ltype);
		}
		auto chunk = duckdb::make_uniq<CV2DataChunk>();
		chunk->Initialize(duckdb::Allocator::DefaultAllocator(), logical_types);
		*out_chunk = Convert(chunk.release());
	});
}

DUCKDB_V2_ERROR duckdb_v2_data_chunk_destroy(duckdb_v2_data_chunk_handle *chunk) {
	return WithErrorHandler(nullptr, [&]() {
		if (!chunk) {
			return;
		}
		if (*chunk) {
			delete Convert(*chunk);
			*chunk = nullptr;
		}
	});
}

DUCKDB_V2_ERROR duckdb_v2_data_chunk_get_size(duckdb_v2_data_chunk_handle chunk, idx_t *out_size,
                                              duckdb_v2_error_info_handle *err) {
	return WithErrorHandler(err, [&]() {
		if (!chunk || !out_size) {
			throw duckdb::InvalidInputException("null argument to duckdb_v2_data_chunk_get_size");
		}
		*out_size = Convert(chunk)->size();
	});
}

DUCKDB_V2_ERROR duckdb_v2_data_chunk_get_vector_count(duckdb_v2_data_chunk_handle chunk, idx_t *out_count,
                                                      duckdb_v2_error_info_handle *err) {
	return WithErrorHandler(err, [&]() {
		if (!chunk || !out_count) {
			throw duckdb::InvalidInputException("null argument to duckdb_v2_data_chunk_get_vector_count");
		}
		*out_count = Convert(chunk)->ColumnCount();
	});
}

DUCKDB_V2_ERROR duckdb_v2_data_chunk_get_vector(duckdb_v2_data_chunk_handle chunk, idx_t index,
                                                duckdb_v2_vector_handle *out_vector, duckdb_v2_error_info_handle *err) {
	return WithErrorHandler(err, [&]() {
		if (!chunk || !out_vector) {
			throw duckdb::InvalidInputException("null argument to duckdb_v2_data_chunk_get_vector");
		}
		*out_vector = nullptr;
		auto *c = Convert(chunk);
		if (index >= c->ColumnCount()) {
			throw duckdb::InvalidInputException("vector index out of range");
		}
		*out_vector = Convert(&c->data[index]);
	});
}
