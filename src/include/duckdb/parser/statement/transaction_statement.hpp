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
	explicit TransactionStatement(TransactionType type);

	unique_ptr<TransactionInfo> info;

public:
	unique_ptr<SQLStatement> Copy() const override;
};
} // namespace duckdb
