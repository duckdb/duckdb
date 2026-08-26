#include "duckdb/main/capi_v2/capi_v2_internal.hpp"

#include "duckdb/common/allocator.hpp"
#include "duckdb/common/type_visitor.hpp"

namespace duckdb {
namespace capiv2 {

static void CreateDataChunk(Allocator &allocator, const duckdb_v2_logical_type_handle *types, idx_t column_count,
                            duckdb_v2_data_chunk_handle *out_chunk, const char *function_name) {
	if (!out_chunk) {
		throw InvalidInputException("null argument to %s", function_name);
	}
	*out_chunk = nullptr;
	if (!types) {
		throw InvalidInputException("null argument to %s", function_name);
	}
	vector<LogicalType> logical_types;
	logical_types.reserve(column_count);
	for (idx_t i = 0; i < column_count; i++) {
		if (!types[i]) {
			throw InvalidInputException("null logical type at index %llu", i);
		}
		const auto &ltype = *Convert(types[i]);
		// ANY is a signature wildcard with no physical layout; a chunk allocates
		// storage, so reject it (an ANY vector throws InternalException).
		if (TypeVisitor::Contains(ltype, LogicalTypeId::ANY)) {
			throw InvalidInputException("logical type at index %llu cannot be ANY", i);
		}
		logical_types.push_back(ltype);
	}
	auto chunk = make_uniq<CV2DataChunk>();
	chunk->Initialize(allocator, logical_types);
	*out_chunk = Convert(chunk.release());
}

static void CopyDataChunk(ClientContext &context, duckdb_v2_data_chunk_handle chunk,
                          duckdb_v2_data_chunk_handle *out_chunk, const char *function_name) {
	if (!out_chunk) {
		throw InvalidInputException("null argument to %s", function_name);
	}
	*out_chunk = nullptr;
	if (!chunk) {
		throw InvalidInputException("null argument to %s", function_name);
	}
	auto &source = *Convert(chunk);
	auto copy = make_uniq<CV2DataChunk>();
	copy->Initialize(Allocator::Get(context), source.GetTypes(), MaxValue<idx_t>(source.size(), STANDARD_VECTOR_SIZE));
	source.Copy(*copy);
	copy->SetCardinalityUnsafe(source.size());
	*out_chunk = Convert(copy.release());
}

} // namespace capiv2
} // namespace duckdb

using namespace duckdb::capiv2;

// TODO: this should be removed.
DUCKDB_V2_ERROR duckdb_v2_data_chunk_create(const duckdb_v2_logical_type_handle *types, idx_t column_count,
                                            duckdb_v2_data_chunk_handle *out_chunk, duckdb_v2_error_info_handle *err) {
	return WithErrorHandler(err, [&]() {
		CreateDataChunk(duckdb::Allocator::DefaultAllocator(), types, column_count, out_chunk,
		                "duckdb_v2_data_chunk_create");
	});
}

DUCKDB_V2_ERROR duckdb_v2_data_chunk_create_with_connection(duckdb_v2_connection_handle conn,
                                                            const duckdb_v2_logical_type_handle *types,
                                                            idx_t column_count, duckdb_v2_data_chunk_handle *out_chunk,
                                                            duckdb_v2_error_info_handle *err) {
	return WithErrorHandler(err, [&]() {
		if (!conn) {
			throw duckdb::InvalidInputException("null argument to duckdb_v2_data_chunk_create_with_connection");
		}
		CreateDataChunk(duckdb::Allocator::Get(*Convert(conn)->context), types, column_count, out_chunk,
		                "duckdb_v2_data_chunk_create_with_connection");
	});
}

DUCKDB_V2_ERROR duckdb_v2_data_chunk_create_with_context(duckdb_v2_context_handle context,
                                                         const duckdb_v2_logical_type_handle *types, idx_t column_count,
                                                         duckdb_v2_data_chunk_handle *out_chunk,
                                                         duckdb_v2_error_info_handle *err) {
	return WithErrorHandler(err, [&]() {
		if (!context) {
			throw duckdb::InvalidInputException("null argument to duckdb_v2_data_chunk_create_with_context");
		}
		CreateDataChunk(duckdb::Allocator::Get(*Convert(context)), types, column_count, out_chunk,
		                "duckdb_v2_data_chunk_create_with_context");
	});
}

DUCKDB_V2_ERROR duckdb_v2_data_chunk_copy_with_connection(duckdb_v2_connection_handle conn,
                                                          duckdb_v2_data_chunk_handle chunk,
                                                          duckdb_v2_data_chunk_handle *out_chunk,
                                                          duckdb_v2_error_info_handle *err) {
	return WithErrorHandler(err, [&]() {
		if (!conn) {
			throw duckdb::InvalidInputException("null argument to duckdb_v2_data_chunk_copy_with_connection");
		}
		CopyDataChunk(*Convert(conn)->context, chunk, out_chunk, "duckdb_v2_data_chunk_copy_with_connection");
	});
}

DUCKDB_V2_ERROR duckdb_v2_data_chunk_copy_with_context(duckdb_v2_context_handle context,
                                                       duckdb_v2_data_chunk_handle chunk,
                                                       duckdb_v2_data_chunk_handle *out_chunk,
                                                       duckdb_v2_error_info_handle *err) {
	return WithErrorHandler(err, [&]() {
		if (!context) {
			throw duckdb::InvalidInputException("null argument to duckdb_v2_data_chunk_copy_with_context");
		}
		CopyDataChunk(*Convert(context), chunk, out_chunk, "duckdb_v2_data_chunk_copy_with_context");
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
