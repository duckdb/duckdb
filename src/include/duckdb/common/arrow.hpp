//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/arrow.hpp
//
//
//===----------------------------------------------------------------------===//

#ifndef ARROW_FLAG_DICTIONARY_ORDERED

#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

#define ARROW_FLAG_DICTIONARY_ORDERED 1
#define ARROW_FLAG_NULLABLE           2
#define ARROW_FLAG_MAP_KEYS_SORTED    4

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
	void (*release)(struct ArrowSchema *) = nullptr;
	// Opaque producer-specific data
	void *private_data;
	~ArrowSchema() {
		if (release) {
			for (int64_t child_idx = 0; child_idx < n_children; child_idx++) {
				auto &child = *children[child_idx];
				if (child.release) {
					child.release(&child);
				}
			}
			release(this);
			release = nullptr;
		}
	}
};

struct ArrowArray {
	// Array data description
	int64_t length = 0;
	int64_t null_count;
	int64_t offset;
	int64_t n_buffers;
	int64_t n_children;
	const void **buffers;
	struct ArrowArray **children;
	struct ArrowArray *dictionary;

	// Release callback
	void (*release)(struct ArrowArray *) = nullptr;
	// Opaque producer-specific data
	void *private_data;

	~ArrowArray() {
		if (release) {
			for (int64_t child_idx = 0; child_idx < n_children; child_idx++) {
				auto &child = *children[child_idx];
				if (child.release) {
					child.release(&child);
				}
			}
			release(this);
		}
	}
};

// EXPERIMENTAL
struct ArrowArrayStream {
	uint64_t number_of_batches = 0;
	uint64_t first_batch_size = 0;
	uint64_t last_batch_size = 0;
	// Callback to get the stream type
	// (will be the same for all arrays in the stream).
	// Return value: 0 if successful, an `errno`-compatible error code otherwise.
	int (*get_schema)(struct ArrowArrayStream *, struct ArrowSchema *out);
	// Callback to get the next array
	// (if no error and the array is released, the stream has ended)
	// Return value: 0 if successful, an `errno`-compatible error code otherwise.
	int (*get_next)(struct ArrowArrayStream *, struct ArrowArray *out, int chunk_idx);

	// Callback to get optional detailed error information.
	// This must only be called if the last stream operation failed
	// with a non-0 return code.  The returned pointer is only valid until
	// the next operation on this stream (including release).
	// If unavailable, NULL is returned.
	const char *(*get_last_error)(struct ArrowArrayStream *);

	// Release callback: release the stream's own resources.
	// Note that arrays returned by `get_next` must be individually released.
	void (*release)(struct ArrowArrayStream *) = nullptr;
	// Opaque producer-specific data
	void *private_data;

	~ArrowArrayStream() {
		if (release) {
			release(this);
			release = nullptr;
		}
	}
};

#ifdef __cplusplus
}
#endif

#endif
