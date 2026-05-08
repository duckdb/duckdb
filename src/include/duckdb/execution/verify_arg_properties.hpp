//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/execution/verify_arg_properties.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/common.hpp"

namespace duckdb {

class BoundCastExpression;
class BoundFunctionExpression;
class DataChunk;
class Vector;

//! In DEBUG builds, verifies declared `ArgProperties` (monotonicity, injectivity) hold over a
//! scalar function call's actual inputs and outputs. No-op in release. Tolerates SPECIAL_HANDLING
//! null behavior — pairs touching a NULL endpoint are skipped.
void VerifyFunctionArgProperties(const BoundFunctionExpression &expr, DataChunk &args, Vector &result);

//! In DEBUG builds, verifies declared `ArgProperties` on a cast (single-arg) hold over the
//! cast's actual source and result. No-op in release.
void VerifyCastArgProperties(const BoundCastExpression &expr, Vector &source, Vector &result, idx_t count);

} // namespace duckdb
