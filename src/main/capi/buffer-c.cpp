
#pragma once
#include <duckdb.h>
#include "duckdb/common/types/vector_buffer.hpp"
#include "duckdb/common/types/vector.hpp"

// extern "C" {
//
// typedef struct ExternalBuffer ExternalBuffer;
//
// typedef void (*external_buffer_free)(ExternalBuffer *buffer);
//
// typedef struct {
// 	void *ptr;
// } CppVectorBuffer;
//
// // These structs and functions are used the wrap a buffer in C++.
// // See convert/array/varbinview::to_duckdb for more
// CppVectorBuffer *NewCppVectorBuffer(ExternalBuffer *buffer, external_buffer_free *free_fn);
//
// void AssignBufferToVec(duckdb_vector vec, CppVectorBuffer *buffer);
// }

class ExternalVectorBuffer : public duckdb::VectorBuffer {
public:
	ExternalVectorBuffer(ExternalBuffer *wrapper, external_buffer_free *free_fn) : wrapper(wrapper), free_fn(free_fn) {
	}

	~ExternalVectorBuffer() override {
		(*free_fn)(wrapper);
	}

private:
	ExternalBuffer *wrapper;
	external_buffer_free *free_fn;
};

typedef struct {
	duckdb::buffer_ptr<ExternalVectorBuffer> buffer;
} CppVectorBufferInternal;

CppVectorBuffer *NewCppVectorBuffer(ExternalBuffer *buffer, external_buffer_free *free_fn) {
	auto rbuffer = duckdb::make_shared_ptr<ExternalVectorBuffer>(buffer, free_fn);
	auto ribuffer = new CppVectorBufferInternal {.buffer = rbuffer};
	return reinterpret_cast<CppVectorBuffer *>(ribuffer);
}

void AssignBufferToVec(duckdb_vector vec, CppVectorBuffer *buffer) {
	auto buf = reinterpret_cast<CppVectorBufferInternal *>(buffer);
	auto dvec = reinterpret_cast<duckdb::Vector *>(vec);
	duckdb::StringVector::AddBuffer(*dvec, buf->buffer);
}