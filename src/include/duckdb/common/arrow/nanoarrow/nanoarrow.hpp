#pragma once

// Ensure Arrow C Data Interface structs are defined before nanoarrow.h so
// nanoarrow skips its own (identical) definitions via ARROW_FLAG_DICTIONARY_ORDERED.
#include "duckdb/common/arrow/arrow.hpp"
// Full upstream nanoarrow (third_party/nanoarrow is in the include path for ADBC targets).
#include "nanoarrow.h"

namespace duckdb {
using ::ArrowBufferAllocator;
using ::ArrowError;
using ::ArrowSchemaView;
using ::ArrowStringView;
} // namespace duckdb
