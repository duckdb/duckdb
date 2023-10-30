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
#include "duckdb/function/create_secret_function.hpp"

namespace duckdb {

//! PhysicalPragma represents the PRAGMA operator
class PhysicalCreateSecret : public PhysicalOperator {
public:
	static constexpr const PhysicalOperatorType TYPE = PhysicalOperatorType::CREATE_SECRET;

public:
	PhysicalCreateSecret(CreateSecretFunction function_p, CreateSecretInfo info_p, idx_t estimated_cardinality)
	    : PhysicalOperator(PhysicalOperatorType::CREATE_SECRET, {LogicalType::BOOLEAN}, estimated_cardinality),
	      function(std::move(function_p)), info(std::move(info_p)) {
	}

	//! The pragma function to call
	CreateSecretFunction function;
	//! The context of the call
	CreateSecretInfo info;

public:
	// Source interface
	SourceResultType GetData(ExecutionContext &context, DataChunk &chunk, OperatorSourceInput &input) const override;

	bool IsSource() const override {
		return true;
	}
};

} // namespace duckdb
