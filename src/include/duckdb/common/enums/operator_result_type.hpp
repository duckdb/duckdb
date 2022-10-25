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

//! OperatorFinalizeResultType is used to indicate whether operators have finished flushing their cached results.
//! FINISHED means the operator has flushed all cached data.
//! HAVE_MORE_OUTPUT means the operator contains more results.
enum class OperatorFinalizeResultType : uint8_t { HAVE_MORE_OUTPUT, FINISHED };

//! The SinkResultType is used to indicate the result of data flowing into a sink
//! There are two possible results:
//! NEED_MORE_INPUT means the sink needs more input
//! FINISHED means the sink is finished executing, and more input will not change the result any further
enum class SinkResultType : uint8_t { NEED_MORE_INPUT, FINISHED };

//! The SinkFinalizeType is used to indicate the result of a Finalize call on a sink
//! There are two possible results:
//! READY means the sink is ready for further processing
//! NO_OUTPUT_POSSIBLE means the sink will never provide output, and any pipelines involving the sink can be skipped
enum class SinkFinalizeType : uint8_t { READY, NO_OUTPUT_POSSIBLE };

} // namespace duckdb
