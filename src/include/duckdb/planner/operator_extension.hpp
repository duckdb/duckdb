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

//! The OperatorExtensionInfo holds static information relevant to the operator extension
struct OperatorExtensionInfo {
	virtual ~OperatorExtensionInfo() {
	}
};

typedef BoundStatement (*bind_function_t)(ClientContext &context, Binder &binder, OperatorExtensionInfo *info,
                                          SQLStatement &statement);

// forward declaration to avoid circular reference
struct LogicalExtensionOperator;

class OperatorExtension {
public:
	bind_function_t Bind; // NOLINT: backwards compatibility

	//! Additional info passed to the CreatePlan & Bind functions
	shared_ptr<OperatorExtensionInfo> operator_info;

	virtual std::string GetName() = 0;
	virtual unique_ptr<LogicalExtensionOperator> Deserialize(Deserializer &deserializer) = 0;

	virtual ~OperatorExtension() {
	}
};

} // namespace duckdb
