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

#pragma once

#include <errno.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "duckdb/common/adbc/adbc.hpp"
#include "duckdb/common/adbc/adbc-init.hpp"

#include "duckdb/common/string.hpp"
#include "duckdb/common/string_util.hpp"

#include "duckdb.h"
#include "duckdb/common/arrow/arrow_wrapper.hpp"
#include "duckdb/common/arrow/arrow.hpp"
#include "duckdb/common/adbc/arrow_type.hpp"
#include "duckdb/common/adbc/arrow_time_unit.hpp"

/// \brief Represents an errno-compatible error code
typedef int ArrowErrorCode;
enum class ArrowErrorCode : int8_t {
	OK = 0,
	NO_MEMORY = 12,
	FILE_EXISTS = 17,
	INVALID_ARGUMENT = 22,
	RESULT_TOO_LARGE = 34
};

static void *ArrowMalloc(int64_t size) {
	return malloc(size);
}

static void ArrowFree(void *ptr) {
	free(ptr);
}

void ArrowSchemaRelease(struct ArrowSchema *schema) {
	if (schema->format != NULL) {
		ArrowFree((void *)schema->format);
	}
	if (schema->name != NULL) {
		ArrowFree((void *)schema->name);
	}
	if (schema->metadata != NULL) {
		ArrowFree((void *)schema->metadata);
	}

	// This object owns the memory for all the children, but those
	// children may have been generated elsewhere and might have
	// their own release() callback.
	if (schema->children != NULL) {
		for (int64_t i = 0; i < schema->n_children; i++) {
			if (schema->children[i] != NULL) {
				if (schema->children[i]->release != NULL) {
					schema->children[i]->release(schema->children[i]);
				}

				ArrowFree(schema->children[i]);
			}
		}

		ArrowFree(schema->children);
	}

	// This object owns the memory for the dictionary but it
	// may have been generated somewhere else and have its own
	// release() callback.
	if (schema->dictionary != NULL) {
		if (schema->dictionary->release != NULL) {
			schema->dictionary->release(schema->dictionary);
		}

		ArrowFree(schema->dictionary);
	}

	// private data not currently used
	if (schema->private_data != NULL) {
		ArrowFree(schema->private_data);
	}

	schema->release = NULL;
}

const char *ArrowSchemaFormatTemplate(ArrowType data_type) {
	switch (data_type) {
	case ArrowType::UNINITIALIZED:
		return NULL;
	case ArrowType::NA:
		return "n";
	case ArrowType::BOOL:
		return "b";

	case ArrowType::UINT8:
		return "C";
	case ArrowType::INT8:
		return "c";
	case ArrowType::UINT16:
		return "S";
	case ArrowType::INT16:
		return "s";
	case ArrowType::UINT32:
		return "I";
	case ArrowType::INT32:
		return "i";
	case ArrowType::UINT64:
		return "L";
	case ArrowType::INT64:
		return "l";

	case ArrowType::HALF_FLOAT:
		return "e";
	case ArrowType::FLOAT:
		return "f";
	case ArrowType::DOUBLE:
		return "g";

	case ArrowType::STRING:
		return "u";
	case ArrowType::LARGE_STRING:
		return "U";
	case ArrowType::BINARY:
		return "z";
	case ArrowType::LARGE_BINARY:
		return "Z";

	case ArrowType::DATE32:
		return "tdD";
	case ArrowType::DATE64:
		return "tdm";
	case ArrowType::INTERVAL_MONTHS:
		return "tiM";
	case ArrowType::INTERVAL_DAY_TIME:
		return "tiD";
	case ArrowType::INTERVAL_MONTH_DAY_NANO:
		return "tin";

	case ArrowType::LIST:
		return "+l";
	case ArrowType::LARGE_LIST:
		return "+L";
	case ArrowType::STRUCT:
		return "+s";
	case ArrowType::MAP:
		return "+m";

	default:
		return NULL;
	}
}

ArrowErrorCode ArrowSchemaInit(struct ArrowSchema *schema, ArrowType data_type) {
	schema->format = NULL;
	schema->name = NULL;
	schema->metadata = NULL;
	schema->flags = ARROW_FLAG_NULLABLE;
	schema->n_children = 0;
	schema->children = NULL;
	schema->dictionary = NULL;
	schema->private_data = NULL;
	schema->release = &ArrowSchemaRelease;

	// We don't allocate the dictionary because it has to be nullptr
	// for non-dictionary-encoded arrays.

	// Set the format to a valid format string for data_type
	const char *template_format = ArrowSchemaFormatTemplate(data_type);

	// If data_type isn't recognized and not explicitly unset
	if (template_format == NULL && data_type != ArrowType::UNINITIALIZED) {
		schema->release(schema);
		return ArrowErrorCode::INVALID_ARGUMENT;
	}

	auto result = ArrowSchemaSetFormat(schema, template_format);
	if (result != ArrowErrorCode::OK) {
		schema->release(schema);
		return result;
	}

	return ArrowErrorCode::OK;
}

ArrowErrorCode ArrowSchemaInitFixedSize(struct ArrowSchema *schema, ArrowType data_type, int32_t fixed_size) {
	auto result = ArrowSchemaInit(schema, ArrowType::UNINITIALIZED);
	if (result != ArrowErrorCode::OK) {
		return result;
	}

	if (fixed_size <= 0) {
		schema->release(schema);
		return ArrowErrorCode::INVALID_ARGUMENT;
	}

	char buffer[64];
	int n_chars;
	switch (data_type) {
	case ArrowType::FIXED_SIZE_BINARY:
		n_chars = snprintf(buffer, sizeof(buffer), "w:%d", (int)fixed_size);
		break;
	case ArrowType::FIXED_SIZE_LIST:
		n_chars = snprintf(buffer, sizeof(buffer), "+w:%d", (int)fixed_size);
		break;
	default:
		schema->release(schema);
		return ArrowErrorCode::INVALID_ARGUMENT;
	}

	buffer[n_chars] = '\0';
	result = ArrowSchemaSetFormat(schema, buffer);
	if (result != ArrowErrorCode::OK) {
		schema->release(schema);
	}

	return result;
}

ArrowErrorCode ArrowSchemaInitDecimal(struct ArrowSchema *schema, ArrowType data_type, int32_t decimal_precision,
                                      int32_t decimal_scale) {
	auto result = ArrowSchemaInit(schema, ArrowType::UNINITIALIZED);
	if (result != ArrowErrorCode::OK) {
		return result;
	}

	if (decimal_precision <= 0) {
		schema->release(schema);
		return ArrowErrorCode::INVALID_ARGUMENT;
	}

	char buffer[64];
	int n_chars;
	switch (data_type) {
	case ArrowType::DECIMAL128:
		n_chars = snprintf(buffer, sizeof(buffer), "d:%d,%d", decimal_precision, decimal_scale);
		break;
	case ArrowType::DECIMAL256:
		n_chars = snprintf(buffer, sizeof(buffer), "d:%d,%d,256", decimal_precision, decimal_scale);
		break;
	default:
		schema->release(schema);
		return ArrowErrorCode::INVALID_ARGUMENT;
	}

	buffer[n_chars] = '\0';

	result = ArrowSchemaSetFormat(schema, buffer);
	if (result != ArrowErrorCode::OK) {
		schema->release(schema);
		return result;
	}

	return ArrowErrorCode::OK;
}

static const char *ArrowTimeUnitString(ArrowTimeUnit time_unit) {
	switch (time_unit) {
	case ArrowTimeUnit::SECOND:
		return "s";
	case ArrowTimeUnit::MILLI:
		return "m";
	case ArrowTimeUnit::MICRO:
		return "u";
	case ArrowTimeUnit::NANO:
		return "n";
	default:
		return NULL;
	}
}

ArrowErrorCode ArrowSchemaInitDateTime(struct ArrowSchema *schema, ArrowType data_type, ArrowTimeUnit time_unit,
                                       const char *timezone) {
	auto result = ArrowSchemaInit(schema, ArrowType::UNINITIALIZED);
	if (result != ArrowErrorCode::OK) {
		return result;
	}

	const char *time_unit_str = ArrowTimeUnitString(time_unit);
	if (time_unit_str == NULL) {
		schema->release(schema);
		return ArrowErrorCode::INVALID_ARGUMENT;
	}

	char buffer[128];
	int n_chars;
	switch (data_type) {
	case ArrowType::TIME32:
	case ArrowType::TIME64:
		if (timezone != NULL) {
			schema->release(schema);
			return ArrowErrorCode::INVALID_ARGUMENT;
		}
		n_chars = snprintf(buffer, sizeof(buffer), "tt%s", time_unit_str);
		break;
	case ArrowType::TIMESTAMP:
		if (timezone == NULL) {
			timezone = "";
		}
		n_chars = snprintf(buffer, sizeof(buffer), "ts%s:%s", time_unit_str, timezone);
		break;
	case ArrowType::DURATION:
		if (timezone != NULL) {
			schema->release(schema);
			return ArrowErrorCode::INVALID_ARGUMENT;
		}
		n_chars = snprintf(buffer, sizeof(buffer), "tD%s", time_unit_str);
		break;
	default:
		schema->release(schema);
		return ArrowErrorCode::INVALID_ARGUMENT;
	}

	if (n_chars >= sizeof(buffer)) {
		schema->release(schema);
		return ArrowErrorCode::RESULT_TOO_LARGE;
	}

	buffer[n_chars] = '\0';

	result = ArrowSchemaSetFormat(schema, buffer);
	if (result != ArrowErrorCode::OK) {
		schema->release(schema);
		return result;
	}

	return ArrowErrorCode::OK;
}

ArrowErrorCode ArrowSchemaSetFormat(struct ArrowSchema *schema, const char *format) {
	if (schema->format != NULL) {
		ArrowFree((void *)schema->format);
	}

	if (format != NULL) {
		size_t format_size = strlen(format) + 1;
		schema->format = (const char *)ArrowMalloc(format_size);
		if (schema->format == NULL) {
			return ArrowErrorCode::NO_MEMORY;
		}

		memcpy((void *)schema->format, format, format_size);
	} else {
		schema->format = NULL;
	}

	return ArrowErrorCode::OK;
}

ArrowErrorCode ArrowSchemaSetName(struct ArrowSchema *schema, const char *name) {
	if (schema->name != NULL) {
		ArrowFree((void *)schema->name);
	}

	if (name != NULL) {
		size_t name_size = strlen(name) + 1;
		schema->name = (const char *)ArrowMalloc(name_size);
		if (schema->name == NULL) {
			return ArrowErrorCode::NO_MEMORY;
		}

		memcpy((void *)schema->name, name, name_size);
	} else {
		schema->name = NULL;
	}

	return ArrowErrorCode::OK;
}

ArrowErrorCode ArrowSchemaSetMetadata(struct ArrowSchema *schema, const char *metadata) {
	if (schema->metadata != NULL) {
		ArrowFree((void *)schema->metadata);
	}

	if (metadata != NULL) {
		size_t metadata_size = ArrowMetadataSizeOf(metadata);
		schema->metadata = (const char *)ArrowMalloc(metadata_size);
		if (schema->metadata == NULL) {
			return ArrowErrorCode::NO_MEMORY;
		}

		memcpy((void *)schema->metadata, metadata, metadata_size);
	} else {
		schema->metadata = NULL;
	}

	return ArrowErrorCode::OK;
}

ArrowErrorCode ArrowSchemaAllocateChildren(struct ArrowSchema *schema, int64_t n_children) {
	if (schema->children != NULL) {
		return ArrowErrorCode::FILE_EXISTS;
	}

	if (n_children > 0) {
		schema->children = (struct ArrowSchema **)ArrowMalloc(n_children * sizeof(struct ArrowSchema *));

		if (schema->children == NULL) {
			return ArrowErrorCode::NO_MEMORY;
		}

		schema->n_children = n_children;

		memset(schema->children, 0, n_children * sizeof(struct ArrowSchema *));

		for (int64_t i = 0; i < n_children; i++) {
			schema->children[i] = (struct ArrowSchema *)ArrowMalloc(sizeof(struct ArrowSchema));

			if (schema->children[i] == NULL) {
				return ArrowErrorCode::NO_MEMORY;
			}

			schema->children[i]->release = NULL;
		}
	}

	return ArrowErrorCode::OK;
}

ArrowErrorCode ArrowSchemaAllocateDictionary(struct ArrowSchema *schema) {
	if (schema->dictionary != NULL) {
		return ArrowErrorCode::FILE_EXISTS;
	}

	schema->dictionary = (struct ArrowSchema *)ArrowMalloc(sizeof(struct ArrowSchema));
	if (schema->dictionary == NULL) {
		return ArrowErrorCode::NO_MEMORY;
	}

	schema->dictionary->release = NULL;
	return ArrowErrorCode::OK;
}

int ArrowSchemaDeepCopy(struct ArrowSchema *schema, struct ArrowSchema *schema_out) {
	auto result;
	result = ArrowSchemaInit(schema_out, ArrowType::NA);
	if (result != ArrowErrorCode::OK) {
		return result;
	}

	result = ArrowSchemaSetFormat(schema_out, schema->format);
	if (result != ArrowErrorCode::OK) {
		schema_out->release(schema_out);
		return result;
	}

	result = ArrowSchemaSetName(schema_out, schema->name);
	if (result != ArrowErrorCode::OK) {
		schema_out->release(schema_out);
		return result;
	}

	result = ArrowSchemaSetMetadata(schema_out, schema->metadata);
	if (result != ArrowErrorCode::OK) {
		schema_out->release(schema_out);
		return result;
	}

	result = ArrowSchemaAllocateChildren(schema_out, schema->n_children);
	if (result != ArrowErrorCode::OK) {
		schema_out->release(schema_out);
		return result;
	}

	for (int64_t i = 0; i < schema->n_children; i++) {
		result = ArrowSchemaDeepCopy(schema->children[i], schema_out->children[i]);
		if (result != ArrowErrorCode::OK) {
			schema_out->release(schema_out);
			return result;
		}
	}

	if (schema->dictionary != NULL) {
		result = ArrowSchemaAllocateDictionary(schema_out);
		if (result != ArrowErrorCode::OK) {
			schema_out->release(schema_out);
			return result;
		}

		result = ArrowSchemaDeepCopy(schema->dictionary, schema_out->dictionary);
		if (result != ArrowErrorCode::OK) {
			schema_out->release(schema_out);
			return result;
		}
	}

	return ArrowErrorCode::OK;
}
