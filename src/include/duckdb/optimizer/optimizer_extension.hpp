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

class Optimizer;
class ClientContext;

//! The OptimizerExtensionInfo holds static information relevant to the optimizer extension
struct OptimizerExtensionInfo {
	virtual ~OptimizerExtensionInfo() {
	}
};

struct OptimizerExtensionInput {
	ClientContext &context;
	Optimizer &optimizer;
	optional_ptr<OptimizerExtensionInfo> info;
};

typedef void (*optimize_function_t)(OptimizerExtensionInput &input, unique_ptr<LogicalOperator> &plan);

class OptimizerExtension {
public:
	//! The optimize function of the optimizer extension.
	//! Takes a logical query plan as an input, which it can modify in place
	optimize_function_t optimize_function;

	//! Additional optimizer info passed to the optimize function
	shared_ptr<OptimizerExtensionInfo> optimizer_info;
};

} // namespace duckdb
