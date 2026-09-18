//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/planner/operator/logical_transaction.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/planner/logical_operator.hpp"
#include "duckdb/parser/parsed_data/transaction_info.hpp"

namespace duckdb {

//! LogicalTransaction represents a transaction statement
class LogicalTransaction : public LogicalOperator {
public:
	static constexpr const LogicalOperatorType TYPE = LogicalOperatorType::LOGICAL_TRANSACTION;

public:
	explicit LogicalTransaction(unique_ptr<TransactionInfo> info);
	~LogicalTransaction() override;

	unique_ptr<TransactionInfo> info;

public:
	void Serialize(Serializer &serializer) const override;
	static unique_ptr<LogicalOperator> Deserialize(Deserializer &deserializer);
	idx_t EstimateCardinality(ClientContext &context) override;

protected:
	void ResolveTypes() override;

private:
	explicit LogicalTransaction(unique_ptr<ParseInfo> info);
};

} // namespace duckdb
