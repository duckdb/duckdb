//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/optimizer/pre_optimizer_extension.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/common.hpp"
#include "duckdb/planner/logical_operator.hpp"

namespace duckdb {

class ClientContext;

//! The PreOptimizerExtensionInfo holds static information relevant to the pre-optimizer extension
struct PreOptimizerExtensionInfo {
	virtual ~PreOptimizerExtensionInfo() = default;
};

struct PreOptimizerExtensionInput {
	ClientContext &context;
};

using pre_optimize_function_t = std::function<void(PreOptimizerExtensionInput &input, unique_ptr<LogicalOperator> &plan)>;

class PreOptimizerExtension {
public:
	pre_optimize_function_t optimize_function;

	shared_ptr<PreOptimizerExtensionInfo> pre_optimizer_info;
};

} // namespace duckdb
