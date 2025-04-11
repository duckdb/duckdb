#include <duckdb.h>
#include "duckdb/common/types/vector_buffer.hpp"
#include "duckdb/common/types/vector.hpp"


// This is a wrapper around an externally managed buffer, which can be assigned to a Vector and
// freed once the vector is done with the buffer.
class ExternalVectorBuffer : public duckdb::VectorBuffer {
public:
	ExternalVectorBuffer(external_buffer buffer, external_buffer_free free_fn) : buffer(buffer), free_fn(free_fn) {
	}

	~ExternalVectorBuffer() override {
		free_fn(buffer);
	}

private:
	external_buffer buffer;
	external_buffer_free free_fn;
};

struct CExternalVectorBuffer {
	duckdb::buffer_ptr<duckdb::VectorBuffer> buffer;

	CExternalVectorBuffer(duckdb::buffer_ptr<duckdb::VectorBuffer> buffer) : buffer(buffer) {}
};


duckdb_vector_buffer duckdb_wrap_external_vector_buffer(external_buffer buffer, external_buffer_free free_fn) {
	auto rbuffer = duckdb::make_shared_ptr<ExternalVectorBuffer>(buffer, free_fn);
	auto ribuffer = new CExternalVectorBuffer (rbuffer);
	return reinterpret_cast<duckdb_vector_buffer>(ribuffer);
}

void duckdb_free_vector_buffer(duckdb_vector_buffer *buffer) {
	if (buffer && *buffer) {
		auto buf = reinterpret_cast<CExternalVectorBuffer *>(*buffer);
		delete buf;
		*buffer = nullptr;
	}
}

void duckdb_assign_buffer_to_vector(duckdb_vector vec, duckdb_vector_buffer buffer) {
	auto buf = reinterpret_cast<CExternalVectorBuffer *>(buffer);
	auto dvec = reinterpret_cast<duckdb::Vector *>(vec);
	duckdb::StringVector::AddBuffer(*dvec, buf->buffer);
}