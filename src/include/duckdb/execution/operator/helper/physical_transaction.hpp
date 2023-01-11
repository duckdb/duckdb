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
	explicit PhysicalTransaction(unique_ptr<TransactionInfo> info, idx_t estimated_cardinality)
	    : PhysicalOperator(PhysicalOperatorType::TRANSACTION, {LogicalType::BOOLEAN}, estimated_cardinality),
	      info(std::move(info)) {
	}

	unique_ptr<TransactionInfo> info;

public:
	void GetData(ExecutionContext &context, DataChunk &chunk, GlobalSourceState &gstate,
	             LocalSourceState &lstate) const override;
};

} // namespace duckdb
