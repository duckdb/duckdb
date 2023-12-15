//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/optimizer/optimizer_extension.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/common.hpp"
#include "duckdb/planner/logical_operator.hpp"

namespace duckdb {

//! The OptimizerExtensionInfo holds static information relevant to the optimizer extension
struct OptimizerExtensionInfo {
	virtual ~OptimizerExtensionInfo() {
	}
};

typedef void (*optimize_function_t)(ClientContext &context, OptimizerExtensionInfo *info,
                                    unique_ptr<LogicalOperator> &plan);

class OptimizerExtension {
public:
	//! The parse function of the parser extension.
	//! Takes a query string as input and returns ParserExtensionParseData (on success) or an error
	optimize_function_t optimize_function;

	//! Additional parser info passed to the parse function
	shared_ptr<OptimizerExtensionInfo> optimizer_info;
};

} // namespace duckdb
