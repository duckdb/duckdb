//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/optimizer/optimizer_extension.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/planner/logical_operator.hpp"

namespace duckdb {

class Optimizer;
class ClientContext;

//! The OptimizerExtensionInfo holds static information relevant to the optimizer extension
struct OptimizerExtensionInfo {
	virtual ~OptimizerExtensionInfo() = default;

	[[nodiscard]] virtual const std::string &GetName() const;
};

struct OptimizerExtensionInput {
	ClientContext &context;
	Optimizer &optimizer;
	optional_ptr<OptimizerExtensionInfo> info;
};

typedef void (*optimize_function_t)(OptimizerExtensionInput &input, unique_ptr<LogicalOperator> &plan);

class OptimizerExtension {
	OptimizerExtension(const optimize_function_t optimize_function_p,
	                   shared_ptr<OptimizerExtensionInfo> optimizer_info_p)
	    : optimize_function(optimize_function_p), optimizer_info(move(optimizer_info_p)) {
		D_ASSERT(optimizer_info);
		D_ASSERT(!optimizer_info->GetName().empty());
	}

public:
	optimize_function_t optimize_function;

	shared_ptr<OptimizerExtensionInfo> optimizer_info;
};

} // namespace duckdb
