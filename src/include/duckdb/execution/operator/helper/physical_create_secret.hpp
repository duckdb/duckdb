//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/execution/operator/helper/physical_pragma.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/execution/physical_operator.hpp"
#include "duckdb/main/secret/secret.hpp"

namespace duckdb {

//! PhysicalCreateSecret represents the CREATE SECRET operator
class PhysicalCreateSecret : public PhysicalOperator {
public:
	static constexpr const PhysicalOperatorType TYPE = PhysicalOperatorType::CREATE_SECRET;

public:
	PhysicalCreateSecret(CreateSecretInput input_p, idx_t estimated_cardinality)
	    : PhysicalOperator(PhysicalOperatorType::CREATE_SECRET, {LogicalType::BOOLEAN}, estimated_cardinality),
	      create_input(std::move(input_p)) {
	}

	CreateSecretInput create_input;

public:
	// Source interface
	SourceResultType GetData(ExecutionContext &context, DataChunk &chunk, OperatorSourceInput &input) const override;

	bool IsSource() const override {
		return true;
	}
};

} // namespace duckdb
