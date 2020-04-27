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
	TransactionStatement(TransactionType type)
	    : SQLStatement(StatementType::TRANSACTION_STATEMENT), info(make_unique<TransactionInfo>(type)){};

	unique_ptr<TransactionInfo> info;
};
} // namespace duckdb
