//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/arrow.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/constants.hpp"

#ifndef ARROW_FLAG_DICTIONARY_ORDERED
// see // see https://github.com/apache/arrow/blob/master/docs/source/format/CDataInterface.rst
#ifdef __cplusplus
extern "C" {
#endif
#define ARROW_FLAG_DICTIONARY_ORDERED 1
#define ARROW_FLAG_NULLABLE 2
#define ARROW_FLAG_MAP_KEYS_SORTED 4

struct ArrowSchema {
	// Array type description
	const char *format;
	const char *name;
	const char *metadata;
	int64_t flags;
	int64_t n_children;
	struct ArrowSchema **children;
	struct ArrowSchema *dictionary;

	// Release callback
	void (*release)(struct ArrowSchema *);
	// Opaque producer-specific data
	void *private_data;
};

struct ArrowArray {
	// Array data description
	int64_t length;
	int64_t null_count;
	int64_t offset;
	int64_t n_buffers;
	int64_t n_children;
	const void **buffers;
	struct ArrowArray **children;
	struct ArrowArray *dictionary;

	// Release callback
	void (*release)(struct ArrowArray *);
	// Opaque producer-specific data
	void *private_data;
};

#ifdef __cplusplus
}
#endif
#endif
// see also https://arrow.apache.org/docs/format/Columnar.html

namespace duckdb {
struct DuckDBArrowTable {

	~DuckDBArrowTable() {
		schema.release(&schema);
		for (duckdb::idx_t chunk_idx = 0; chunk_idx < chunks.size(); chunk_idx++) {
			chunks[chunk_idx].release(&chunks[chunk_idx]);
		}
	}
	ArrowSchema schema;
	std::vector<ArrowArray> chunks; // with chunks
};
} // namespace duckdb
