//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/execution/operator/helper/physical_pragma.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/execution/physical_operator.hpp"
#include "duckdb/parser/parsed_data/bound_pragma_info.hpp"

namespace duckdb {

//! PhysicalPragma represents the PRAGMA operator
class PhysicalPragma : public PhysicalOperator {
public:
	static constexpr const PhysicalOperatorType TYPE = PhysicalOperatorType::PRAGMA;

public:
	PhysicalPragma(unique_ptr<BoundPragmaInfo> info_p, idx_t estimated_cardinality)
	    : PhysicalOperator(PhysicalOperatorType::PRAGMA, {LogicalType::BOOLEAN}, estimated_cardinality),
	      info(std::move(info_p)) {
	}

	//! The context of the call
	unique_ptr<BoundPragmaInfo> info;

public:
	// Source interface
	SourceResultType GetData(ExecutionContext &context, DataChunk &chunk, OperatorSourceInput &input) const override;

	bool IsSource() const override {
		return true;
	}
};

} // namespace duckdb
