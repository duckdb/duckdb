//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/execution/operator/helper/physical_transaction.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/execution/physical_operator.hpp"
#include "duckdb/parser/parsed_data/transaction_info.hpp"

namespace duckdb {

//! PhysicalTransaction represents a transaction operator (e.g. BEGIN or COMMIT)
class PhysicalTransaction : public PhysicalOperator {
public:
	static constexpr const PhysicalOperatorType TYPE = PhysicalOperatorType::TRANSACTION;

public:
	explicit PhysicalTransaction(unique_ptr<TransactionInfo> info, idx_t estimated_cardinality)
	    : PhysicalOperator(PhysicalOperatorType::TRANSACTION, {LogicalType::BOOLEAN}, estimated_cardinality),
	      info(std::move(info)) {
	}

	unique_ptr<TransactionInfo> info;

public:
	SourceResultType GetData(ExecutionContext &context, DataChunk &chunk, OperatorSourceInput &input) const override;

	bool IsSource() const override {
		return true;
	}
};

} // namespace duckdb
