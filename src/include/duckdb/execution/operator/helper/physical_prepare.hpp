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
	PhysicalPrepare(string name, shared_ptr<PreparedStatementData> prepared, idx_t estimated_cardinality)
	    : PhysicalOperator(PhysicalOperatorType::PREPARE, {LogicalType::BOOLEAN}, estimated_cardinality), name(name),
	      prepared(std::move(prepared)) {
	}

	string name;
	shared_ptr<PreparedStatementData> prepared;

public:
	// Source interface
	void GetData(ExecutionContext &context, DataChunk &chunk, GlobalSourceState &gstate,
	             LocalSourceState &lstate) const override;
};

} // namespace duckdb
