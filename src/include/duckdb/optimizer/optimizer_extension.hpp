//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/optimizer/optimizer_extension.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/common.hpp"
#include "duckdb/common/enums/optimizer_type.hpp"
#include "duckdb/planner/logical_operator.hpp"
#include "duckdb/main/extension_callback_manager.hpp"

namespace duckdb {
struct DBConfig;
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
typedef void (*pre_optimize_function_t)(OptimizerExtensionInput &input, unique_ptr<LogicalOperator> &plan);

//! Position of an anchored rule relative to the built-in pass it targets.
enum class OptimizerHookPosition : uint8_t { Before, After };

class OptimizerExtension {
public:
	//! The optimize function of the optimizer extension.
	//! Takes a logical query plan as an input, which it can modify in place
	//! This runs, after the DuckDB optimizers have run
	optimize_function_t optimize_function = nullptr;
	//! The pre-optimize function of the optimizer extension.
	//! Takes a logical query plan as an input, which it can modify in place
	//! This runs, before the DuckDB optimizers have run
	pre_optimize_function_t pre_optimize_function = nullptr;

	//! Anchored rule: fires `where` the built-in pass `anchor` runs.
	//! Use this when the rewrite needs to compose with a specific pass --
	//! e.g. {anchor=FILTER_PUSHDOWN, where=Before} fires just before
	//! FILTER_PUSHDOWN. `rule` is null in unused slots; the dispatcher skips.
	optimize_function_t rule = nullptr;
	OptimizerType anchor = OptimizerType::INVALID;
	OptimizerHookPosition where = OptimizerHookPosition::After;

	//! Additional optimizer info passed to the optimize functions
	shared_ptr<OptimizerExtensionInfo> optimizer_info;

	static void Register(DBConfig &config, OptimizerExtension extension);
	static ExtensionCallbackIteratorHelper<OptimizerExtension> Iterate(ClientContext &context) {
		return ExtensionCallbackManager::Get(context).OptimizerExtensions();
	}
};

} // namespace duckdb
