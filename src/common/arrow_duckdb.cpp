#include "duckdb/common/exception.hpp"
#include "duckdb/common/assert.hpp"
#include "duckdb/common/arrow_duckdb.hpp"
namespace duckdb {

ArrowSchemaDuck::~ArrowSchemaDuck() {
	if (arrow_schema.release) {
		for (int64_t child_idx = 0; child_idx < arrow_schema.n_children; child_idx++) {
			auto &child = *arrow_schema.children[child_idx];
			if (child.release) {
				child.release(&child);
			}
		}
		arrow_schema.release(&arrow_schema);
		arrow_schema.release = nullptr;
	}
}

ArrowArrayDuck::~ArrowArrayDuck() {
	if (arrow_array.release) {
		for (int64_t child_idx = 0; child_idx < arrow_array.n_children; child_idx++) {
			auto &child = *arrow_array.children[child_idx];
			if (child.release) {
				child.release(&child);
			}
		}
		arrow_array.release(&arrow_array);
		arrow_array.release = nullptr;
	}
}

ArrowArrayStreamDuck::~ArrowArrayStreamDuck() {
	if (arrow_array_stream.release) {
		arrow_array_stream.release(&arrow_array_stream);
		arrow_array_stream.release = nullptr;
	}
}

void ArrowArrayStreamDuck::GetSchema(ArrowSchemaDuck &schema) {
	D_ASSERT(arrow_array_stream.get_schema);
	if (arrow_array_stream.get_schema(&arrow_array_stream, &schema.arrow_schema)) {
		throw InvalidInputException("arrow_scan: get_schema failed(): %s", string(GetError()));
	}
	if (!schema.arrow_schema.release) {
		throw InvalidInputException("arrow_scan: released schema passed");
	}

	if (schema.arrow_schema.n_children < 1) {
		throw InvalidInputException("arrow_scan: empty schema passed");
	}
}

unique_ptr<ArrowArrayDuck> ArrowArrayStreamDuck::GetNextChunk() {
	auto current_chunk = make_unique<ArrowArrayDuck>();
	if (arrow_array_stream.get_next(&arrow_array_stream, &current_chunk->arrow_array)) {
		throw InvalidInputException("arrow_scan: get_next failed(): %s", string(GetError()));
	}

	return current_chunk;
}

const char *ArrowArrayStreamDuck::GetError() {
	return arrow_array_stream.get_last_error(&arrow_array_stream);
}

} // namespace duckdb