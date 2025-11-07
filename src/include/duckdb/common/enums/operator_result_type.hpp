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
//! There are four possible results:
//! NEED_MORE_INPUT means the operator is done with the current input and can consume more input if available
//! If there is more input the operator will be called with more input, otherwise the operator will not be called again.
//! HAVE_MORE_OUTPUT means the operator is not finished yet with the current input.
//! The operator will be called again with the same input.
//! FINISHED means the operator has finished the entire pipeline and no more processing is necessary.
//! The operator will not be called again, and neither will any other operators in this pipeline.
//! BLOCKED means the operator does not want to be called right now. e.g. because its currently doing async I/O. The
//! operator has set the interrupt state and the caller is expected to handle it. Note that intermediate operators
//! should currently not emit this state.
enum class OperatorResultType : uint8_t { NEED_MORE_INPUT, HAVE_MORE_OUTPUT, FINISHED, BLOCKED };

//! OperatorFinalizeResultType is used to indicate whether operators have finished flushing their cached results.
//! FINISHED means the operator has flushed all cached data.
//! HAVE_MORE_OUTPUT means the operator contains more results.
enum class OperatorFinalizeResultType : uint8_t { HAVE_MORE_OUTPUT, FINISHED };

//! OperatorFinalResultType is used for the final call
enum class OperatorFinalResultType : uint8_t { FINISHED, BLOCKED };

//! SourceResultType is used to indicate the result of data being pulled out of a source.
//! There are three possible results:
//! HAVE_MORE_OUTPUT means the source has more output, this flag should only be set when data is returned, empty results
//! should only occur for the FINISHED and BLOCKED flags
//! FINISHED means the source is exhausted
//! BLOCKED means the source is currently blocked, e.g. by some async I/O
enum class SourceResultType : uint8_t { HAVE_MORE_OUTPUT, FINISHED, BLOCKED };

//! AsyncResultType is used to indicate the result of a AsyncResult, in the context of a wider operation being executed
enum class AsyncResultType : uint8_t {
	INVALID,  // current result is in an invalid state (eg: it's in the process of being initialized)
	IMPLICIT, // current result depends on external context (eg: in the context of TableFunctions, either FINISHED or
	          // HAVE_MORE_OUTPUT depending on output_chunk.size())
	HAVE_MORE_OUTPUT, // current result is not completed, finished (eg: in the context of TableFunctions, function
	                  // accept more iterations and might produce further results)
	FINISHED,         // current result is completed, no subsequent calls on the same state should be attempted
	BLOCKED // current result is blocked, no subsequent calls on the same state should be attempted (eg: in the context
	        // of AsyncResult, BLOCKED will be associated with a vector of AsyncTasks to be scheduled)
};

bool ExtractSourceResultType(AsyncResultType in, SourceResultType &out);

//! The SinkResultType is used to indicate the result of data flowing into a sink
//! There are three possible results:
//! NEED_MORE_INPUT means the sink needs more input
//! FINISHED means the sink is finished executing, and more input will not change the result any further
//! BLOCKED means the sink is currently blocked, e.g. by some async I/O.
enum class SinkResultType : uint8_t { NEED_MORE_INPUT, FINISHED, BLOCKED };

// todo comment
enum class SinkCombineResultType : uint8_t { FINISHED, BLOCKED };

//! The SinkFinalizeType is used to indicate the result of a Finalize call on a sink
//! There are two possible results:
//! READY means the sink is ready for further processing
//! NO_OUTPUT_POSSIBLE means the sink will never provide output, and any pipelines involving the sink can be skipped
//! BLOCKED means the finalize call to the sink is currently blocked, e.g. by some async I/O.
enum class SinkFinalizeType : uint8_t { READY, NO_OUTPUT_POSSIBLE, BLOCKED };

//! The SinkNextBatchType is used to indicate the result of a NextBatch call on a sink
//! There are two possible results:
//! READY means the sink is ready for further processing
//! BLOCKED means the NextBatch call to the sink is currently blocked, e.g. by some async I/O.
enum class SinkNextBatchType : uint8_t { READY, BLOCKED };

} // namespace duckdb
