//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/planner/operator/logical_update_extensions.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/planner/logical_operator.hpp"
#include "duckdb/parser/parsed_data/update_extensions_info.hpp"

namespace duckdb {

//! LogicalUpdateExtensions represents an UPDATE EXTENSIONS statement
class LogicalUpdateExtensions : public LogicalOperator {
public:
	static constexpr const LogicalOperatorType TYPE = LogicalOperatorType::LOGICAL_UPDATE_EXTENSIONS;

public:
	explicit LogicalUpdateExtensions(unique_ptr<UpdateExtensionsInfo> info);
	~LogicalUpdateExtensions() override;

	unique_ptr<UpdateExtensionsInfo> info;

public:
	idx_t EstimateCardinality(ClientContext &context) override;

protected:
	void ResolveTypes() override;
};

} // namespace duckdb
