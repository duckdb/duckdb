//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/execution/operator/helper/physical_prepare.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/execution/physical_operator.hpp"
#include "duckdb/common/enums/physical_operator_type.hpp"
#include "duckdb/main/prepared_statement_data.hpp"

namespace duckdb {

class PhysicalPrepare : public PhysicalOperator {
public:
	static constexpr const PhysicalOperatorType TYPE = PhysicalOperatorType::PREPARE;

public:
	PhysicalPrepare(string name_p, shared_ptr<PreparedStatementData> prepared, idx_t estimated_cardinality)
	    : PhysicalOperator(PhysicalOperatorType::PREPARE, {LogicalType::BOOLEAN}, estimated_cardinality),
	      name(std::move(name_p)), prepared(std::move(prepared)) {
	}

	string name;
	shared_ptr<PreparedStatementData> prepared;

public:
	// Source interface
	SourceResultType GetData(ExecutionContext &context, DataChunk &chunk, OperatorSourceInput &input) const override;

	bool IsSource() const override {
		return true;
	}
};

} // namespace duckdb
