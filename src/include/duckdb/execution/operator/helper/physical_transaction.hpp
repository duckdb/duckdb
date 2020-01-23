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
	PhysicalTransaction(unique_ptr<TransactionInfo> info)
	    : PhysicalOperator(PhysicalOperatorType::TRANSACTION, {TypeId::BOOL}), info(move(info)) {
	}

	unique_ptr<TransactionInfo> info;

public:
	void GetChunkInternal(ClientContext &context, DataChunk &chunk, PhysicalOperatorState *state) override;
};

} // namespace duckdb
