// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#include <errno.h>
#include <stdlib.h>
#include <string.h>

#include "duckdb/common/arrow/nanoarrow/nanoarrow.hpp"

namespace duckdb_nanoarrow {

ArrowErrorCode ArrowMetadataReaderInit(struct ArrowMetadataReader *reader, const char *metadata) {
	reader->metadata = metadata;

	if (reader->metadata == NULL) {
		reader->offset = 0;
		reader->remaining_keys = 0;
	} else {
		memcpy(&reader->remaining_keys, reader->metadata, sizeof(int32_t));
		reader->offset = sizeof(int32_t);
	}

	return NANOARROW_OK;
}

ArrowErrorCode ArrowMetadataReaderRead(struct ArrowMetadataReader *reader, struct ArrowStringView *key_out,
                                       struct ArrowStringView *value_out) {
	if (reader->remaining_keys <= 0) {
		return EINVAL;
	}

	int64_t pos = 0;

	int32_t key_size;
	memcpy(&key_size, reader->metadata + reader->offset + pos, sizeof(int32_t));
	pos += sizeof(int32_t);

	key_out->data = reader->metadata + reader->offset + pos;
	key_out->n_bytes = key_size;
	pos += key_size;

	int32_t value_size;
	memcpy(&value_size, reader->metadata + reader->offset + pos, sizeof(int32_t));
	pos += sizeof(int32_t);

	value_out->data = reader->metadata + reader->offset + pos;
	value_out->n_bytes = value_size;
	pos += value_size;

	reader->offset += pos;
	reader->remaining_keys--;
	return NANOARROW_OK;
}

int64_t ArrowMetadataSizeOf(const char *metadata) {
	if (metadata == NULL) {
		return 0;
	}

	struct ArrowMetadataReader reader;
	struct ArrowStringView key;
	struct ArrowStringView value;
	ArrowMetadataReaderInit(&reader, metadata);

	int64_t size = sizeof(int32_t);
	while (ArrowMetadataReaderRead(&reader, &key, &value) == NANOARROW_OK) {
		size += sizeof(int32_t) + key.n_bytes + sizeof(int32_t) + value.n_bytes;
	}

	return size;
}

ArrowErrorCode ArrowMetadataGetValue(const char *metadata, const char *key, const char *default_value,
                                     struct ArrowStringView *value_out) {
	struct ArrowStringView target_key_view = {key, static_cast<int64_t>(strlen(key))};
	value_out->data = default_value;
	if (default_value != NULL) {
		value_out->n_bytes = strlen(default_value);
	} else {
		value_out->n_bytes = 0;
	}

	struct ArrowMetadataReader reader;
	struct ArrowStringView key_view;
	struct ArrowStringView value;
	ArrowMetadataReaderInit(&reader, metadata);

	while (ArrowMetadataReaderRead(&reader, &key_view, &value) == NANOARROW_OK) {
		int key_equal = target_key_view.n_bytes == key_view.n_bytes &&
		                strncmp(target_key_view.data, key_view.data, key_view.n_bytes) == 0;
		if (key_equal) {
			value_out->data = value.data;
			value_out->n_bytes = value.n_bytes;
			break;
		}
	}

	return NANOARROW_OK;
}

char ArrowMetadataHasKey(const char *metadata, const char *key) {
	struct ArrowStringView value;
	ArrowMetadataGetValue(metadata, key, NULL, &value);
	return value.data != NULL;
}

} // namespace duckdb_nanoarrow
