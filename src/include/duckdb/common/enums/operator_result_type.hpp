//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/enums/operator_result_type.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/constants.hpp"

namespace duckdb {

//! The OperatorResultType is used to indicate how data should flow around a regular (i.e. non-sink and non-source)
//! physical operator
//! There are three possible results:
//! NEED_MORE_INPUT means the operator is done with the current input and can consume more input if available
//! If there is more input the operator will be called with more input, otherwise the operator will not be called again.
//! HAVE_MORE_OUTPUT means the operator is not finished yet with the current input.
//! The operator will be called again with the same input.
//! FINISHED means the operator has finished the entire pipeline and no more processing is necessary.
//! The operator will not be called again, and neither will any other operators in this pipeline.
enum class OperatorResultType : uint8_t { NEED_MORE_INPUT, HAVE_MORE_OUTPUT, FINISHED };

enum class SinkResultType : uint8_t { NEED_MORE_INPUT, FINISHED };

} // namespace duckdb
