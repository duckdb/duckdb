//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/planner/operator_extension.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/common.hpp"
#include "duckdb/execution/physical_plan_generator.hpp"
#include "duckdb/planner/binder.hpp"

namespace duckdb {

// forward declaration to break circular inclusion
class LogicalExtensionOperator;

//! The OperatorExtensionInfo holds static information relevant to the operator extension
struct OperatorExtensionInfo {
	DUCKDB_API virtual ~OperatorExtensionInfo() {
	}
};

typedef unique_ptr<PhysicalOperator> (*create_plan_function_t)(ClientContext &context, PhysicalPlanGenerator &generator,
                                                               LogicalExtensionOperator &plan);
typedef BoundStatement (*bind_function_t)(ClientContext &context, Binder &binder, OperatorExtensionInfo *info,
                                          SQLStatement &statement);

class OperatorExtension {
public:
	//! Creates a plan for a new logical operator injected into duckdb via the OperatorExtension's bind function or
	//  the optimizer extension
	create_plan_function_t CreatePlan;
	bind_function_t Bind;

	//! Additional info passed to the CreatePlan & Bind functions
	shared_ptr<OperatorExtensionInfo> operator_info;

	DUCKDB_API virtual ~OperatorExtension() {
	}
};

} // namespace duckdb
