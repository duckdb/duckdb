//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/execution/operator/persistent/physical_copy_database.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/execution/physical_operator.hpp"
#include "duckdb/planner/operator/logical_copy_database.hpp"

namespace duckdb {

class PhysicalCopyDatabase : public PhysicalOperator {
public:
	static constexpr const PhysicalOperatorType TYPE = PhysicalOperatorType::COPY_DATABASE;

public:
	PhysicalCopyDatabase(PhysicalPlan &physical_plan, vector<LogicalType> types, idx_t estimated_cardinality,
	                     unique_ptr<CopyDatabaseInfo> info_p);
	~PhysicalCopyDatabase() override;

	unique_ptr<CopyDatabaseInfo> info;

public:
	// Source interface
	SourceResultType GetDataInternal(ExecutionContext &context, DataChunk &chunk,
	                                 OperatorSourceInput &input) const override;

	bool IsSource() const override {
		return true;
	}
};

} // namespace duckdb
