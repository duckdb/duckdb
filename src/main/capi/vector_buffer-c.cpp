#include <duckdb.h>
#include "duckdb/common/types/vector_buffer.hpp"
#include "duckdb/common/types/vector.hpp"

// This is a wrapper around an opaque managed buffer, which can be assigned to a Vector and
// freed once the vector is done with the buffer.
class OpaqueVectorBuffer : public duckdb::VectorBuffer {
public:
	OpaqueVectorBuffer(opaque_buffer buffer, opaque_buffer_free free_fn) : buffer(buffer), free_fn(free_fn) {
	}

	~OpaqueVectorBuffer() override {
		free_fn(buffer);
	}

private:
	opaque_buffer buffer;
	opaque_buffer_free free_fn;
};

struct COpaqueVectorBuffer {
	duckdb::buffer_ptr<duckdb::VectorBuffer> buffer;

	explicit COpaqueVectorBuffer(duckdb::buffer_ptr<duckdb::VectorBuffer> buffer) : buffer(buffer) {
	}
};

duckdb_vector_buffer duckdb_wrap_opaque_buffer_as_vector_buffer(opaque_buffer buffer, opaque_buffer_free free_fn) {
	auto opaque_buffer = duckdb::make_shared_ptr<OpaqueVectorBuffer>(buffer, free_fn);
	auto c_opaque_buffer = new COpaqueVectorBuffer(opaque_buffer);
	return reinterpret_cast<duckdb_vector_buffer>(c_opaque_buffer);
}

void duckdb_free_vector_buffer(duckdb_vector_buffer *buffer) {
	if (buffer && *buffer) {
		auto buf = reinterpret_cast<COpaqueVectorBuffer *>(*buffer);
		delete buf;
		*buffer = nullptr;
	}
}

void duckdb_assign_buffer_to_vector(duckdb_vector vec, duckdb_vector_buffer buffer) {
	auto buf = reinterpret_cast<COpaqueVectorBuffer *>(buffer);
	auto dvec = reinterpret_cast<duckdb::Vector *>(vec);
	duckdb::StringVector::AddBuffer(*dvec, buf->buffer);
}
