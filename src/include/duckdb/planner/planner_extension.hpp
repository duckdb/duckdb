//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/planner/planner_extension.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/common.hpp"
#include "duckdb/planner/bound_statement.hpp"
#include "duckdb/main/extension_callback_manager.hpp"

namespace duckdb {
struct DBConfig;
class Binder;
class ClientContext;

//! The PlannerExtensionInfo holds static information relevant to the planner extension
struct PlannerExtensionInfo {
	virtual ~PlannerExtensionInfo() {
	}
};

struct PlannerExtensionInput {
	ClientContext &context;
	Binder &binder;
	optional_ptr<PlannerExtensionInfo> info;
};

//! The post_bind function runs after binding succeeds, allowing modification of the bound statement
typedef void (*post_bind_function_t)(PlannerExtensionInput &input, BoundStatement &statement);

class PlannerExtension {
public:
	//! The post-bind function of the planner extension.
	//! Takes a bound statement as input, which it can modify in place.
	//! This runs after the binder has successfully bound the statement,
	//! allowing modification of the plan and result types.
	post_bind_function_t post_bind_function = nullptr;

	//! Additional planner info passed to the functions
	shared_ptr<PlannerExtensionInfo> planner_info;

	static void Register(DBConfig &config, PlannerExtension extension);
	static ExtensionCallbackIteratorHelper<PlannerExtension> Iterate(ClientContext &context) {
		return ExtensionCallbackManager::Get(context).PlannerExtensions();
	}
};

} // namespace duckdb
