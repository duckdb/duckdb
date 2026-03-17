#include "duckdb/common/adbc/single_batch_array_stream.hpp"
#include "duckdb/common/arrow/nanoarrow/nanoarrow.h"
#include "duckdb/common/adbc/adbc.hpp"

#include "duckdb.h"
#include "duckdb/common/arrow/arrow_wrapper.hpp"
#include "duckdb/common/arrow/nanoarrow/nanoarrow.hpp"

#include <errno.h>
#include <stdarg.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

namespace duckdb_adbc {

using duckdb_nanoarrow::ArrowSchemaDeepCopy;

static const char *SingleBatchArrayStreamGetLastError(struct ArrowArrayStream *stream) {
	return NULL;
}

static int SingleBatchArrayStreamGetNext(struct ArrowArrayStream *stream, struct ArrowArray *batch) {
	if (!stream || !stream->private_data) {
		return EINVAL;
	}
	struct SingleBatchArrayStream *impl = (struct SingleBatchArrayStream *)stream->private_data;

	memcpy(batch, &impl->batch, sizeof(*batch));
	memset(&impl->batch, 0, sizeof(*batch));
	return 0;
}

static int SingleBatchArrayStreamGetSchema(struct ArrowArrayStream *stream, struct ArrowSchema *schema) {
	if (!stream || !stream->private_data) {
		return EINVAL;
	}
	struct SingleBatchArrayStream *impl = (struct SingleBatchArrayStream *)stream->private_data;

	return ArrowSchemaDeepCopy(&impl->schema, schema);
}

static void SingleBatchArrayStreamRelease(struct ArrowArrayStream *stream) {
	if (!stream || !stream->private_data) {
		return;
	}
	struct SingleBatchArrayStream *impl = (struct SingleBatchArrayStream *)stream->private_data;
	impl->schema.release(&impl->schema);
	if (impl->batch.release) {
		impl->batch.release(&impl->batch);
	}
	free(impl);

	memset(stream, 0, sizeof(*stream));
}

AdbcStatusCode BatchToArrayStream(struct ArrowArray *values, struct ArrowSchema *schema,
                                  struct ArrowArrayStream *stream, struct AdbcError *error) {
	if (!values->release) {
		SetError(error, "ArrowArray is not initialized");
		return ADBC_STATUS_INTERNAL;
	} else if (!schema->release) {
		SetError(error, "ArrowSchema is not initialized");
		return ADBC_STATUS_INTERNAL;
	} else if (stream->release) {
		SetError(error, "ArrowArrayStream is already initialized");
		return ADBC_STATUS_INTERNAL;
	}

	struct SingleBatchArrayStream *impl = (struct SingleBatchArrayStream *)malloc(sizeof(*impl));
	memcpy(&impl->schema, schema, sizeof(*schema));
	memcpy(&impl->batch, values, sizeof(*values));
	memset(schema, 0, sizeof(*schema));
	memset(values, 0, sizeof(*values));
	stream->private_data = impl;
	stream->get_last_error = SingleBatchArrayStreamGetLastError;
	stream->get_next = SingleBatchArrayStreamGetNext;
	stream->get_schema = SingleBatchArrayStreamGetSchema;
	stream->release = SingleBatchArrayStreamRelease;

	return ADBC_STATUS_OK;
}

} // namespace duckdb_adbc
