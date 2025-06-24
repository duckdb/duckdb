//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/parser/statement/transaction_statement.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/parser/sql_statement.hpp"
#include "duckdb/parser/parsed_data/transaction_info.hpp"

namespace duckdb {

class TransactionStatement : public SQLStatement {
public:
	static constexpr const StatementType TYPE = StatementType::TRANSACTION_STATEMENT;

public:
	explicit TransactionStatement(unique_ptr<TransactionInfo> info);

	unique_ptr<TransactionInfo> info;

protected:
	TransactionStatement(const TransactionStatement &other);

public:
	unique_ptr<SQLStatement> Copy() const override;
	string ToString() const override;
};
} // namespace duckdb
