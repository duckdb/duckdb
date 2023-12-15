#pragma once

#include "duckdb/common/arrow/nanoarrow/nanoarrow.h"

// Bring in the symbols from duckdb_nanoarrow into duckdb
namespace duckdb {

// using duckdb_nanoarrow::ArrowBuffer; //We have a variant of this that should be renamed
using duckdb_nanoarrow::ArrowBufferAllocator;
using duckdb_nanoarrow::ArrowError;
using duckdb_nanoarrow::ArrowSchemaView;
using duckdb_nanoarrow::ArrowStringView;

} // namespace duckdb
