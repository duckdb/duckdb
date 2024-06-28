//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/execution/operator/helper/physical_pragma.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/execution/physical_operator.hpp"
#include "duckdb/parser/parsed_data/create_secret_info.hpp"

namespace duckdb {

//! PhysicalCreateSecret represents the CREATE SECRET operator
class PhysicalCreateSecret : public PhysicalOperator {
public:
	static constexpr const PhysicalOperatorType TYPE = PhysicalOperatorType::CREATE_SECRET;

public:
	PhysicalCreateSecret(CreateSecretInfo info_p, idx_t estimated_cardinality)
	    : PhysicalOperator(PhysicalOperatorType::CREATE_SECRET, {LogicalType::BOOLEAN}, estimated_cardinality),
	      info(std::move(info_p)) {
	}

	CreateSecretInfo info;

public:
	// Source interface
	SourceResultType GetData(ExecutionContext &context, DataChunk &chunk, OperatorSourceInput &input) const override;

	bool IsSource() const override {
		return true;
	}
};

} // namespace duckdb
