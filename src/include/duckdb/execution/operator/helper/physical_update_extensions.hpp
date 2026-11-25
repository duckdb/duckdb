//===----------------------------------------------------------------------===//
//                         DuckDB
//
// src/include/duckdb/execution/operator/helper/physical_update_extensions.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/execution/physical_operator.hpp"
#include "duckdb/parser/parsed_data/update_extensions_info.hpp"
#include "duckdb/main/extension_helper.hpp"

namespace duckdb {

struct UpdateExtensionsGlobalState : public GlobalSourceState {
	UpdateExtensionsGlobalState() : updates_scheduled(false), offset(0) {
	}

	//! One entry per extension, pending updates fill in their entry when they are executed
	vector<ExtensionUpdateResult> update_result_entries;
	//! The extensions that need a download, executed concurrently as async tasks
	vector<PendingExtensionUpdate> pending_updates;
	//! Whether the pending updates have been scheduled
	bool updates_scheduled;
	idx_t offset;
};

//! PhysicalUpdateExtensions represents an extension UPDATE operation
class PhysicalUpdateExtensions : public PhysicalOperator {
public:
	static constexpr const PhysicalOperatorType TYPE = PhysicalOperatorType::UPDATE_EXTENSIONS;

public:
	explicit PhysicalUpdateExtensions(PhysicalPlan &physical_plan, unique_ptr<UpdateExtensionsInfo> info,
	                                  idx_t estimated_cardinality)
	    : PhysicalOperator(physical_plan, PhysicalOperatorType::UPDATE_EXTENSIONS,
	                       {LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::VARCHAR,
	                        LogicalType::VARCHAR, LogicalType::VARCHAR},
	                       estimated_cardinality),
	      info(std::move(info)) {
	}

	unique_ptr<UpdateExtensionsInfo> info;

	unique_ptr<GlobalSourceState> GetGlobalSourceState(ClientContext &context) const override;

public:
	// Source interface
	SourceResultType GetDataInternal(ExecutionContext &context, DataChunk &chunk,
	                                 OperatorSourceInput &input) const override;

	bool IsSource() const override {
		return true;
	}
};

} // namespace duckdb
